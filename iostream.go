package chunkflow

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
)

// IoStream represents a lazily evaluated, context-aware pipeline whose elements may
// carry errors. It mirrors the Stream API; every method that accepts a user callback
// has an *Ctx counterpart that receives a context and may return an error.
//
// Error semantics: an error produced by the source or by a callback travels down the
// pipeline as an element. Intermediate operations pass it through untouched and keep
// processing the remaining input; they operate on values only and never interpret
// errors. CircuitBreaker may mark an error as tolerated (see ErrSuppressed). Terminal
// operations skip elements whose error matches ErrSuppressed and stop at the first
// other error, so a pipeline without a CircuitBreaker behaves as "fail fast".
type IoStream[T any] struct {
	options options
	ctx     context.Context
	seq     iter.Seq[result[T]]
}

// Option configures an IoStream or a single asynchronous operation.
// Option configures a whole pipeline. Pass it to NewIo or Opts; every operation
// downstream inherits it. Every StepOption is also an Option.
type Option interface {
	applyPipeline(*options)
}

// StepOption configures a single *Ctx call, e.g. MapCtx(fn, WithParallel(8)). Passing it
// to NewIo or Opts instead makes it the default for the whole pipeline.
type StepOption func(*options)

func (f StepOption) applyPipeline(o *options) { f(o) }

// pipelineOption is an Option that makes no sense for a single step.
type pipelineOption func(*options)

func (f pipelineOption) applyPipeline(o *options) { f(o) }

// WithParallel sets the number of concurrent workers used by MapCtx, FilterCtx and TapCtx.
// With n > 1 the output order of that step is not guaranteed. A value below 1 is invalid:
// the stream it is applied to emits a single error and ends.
func WithParallel(concurrency int) StepOption {
	return func(o *options) {
		if concurrency < 1 {
			o.err = fmt.Errorf("chunkflow: WithParallel(%d): concurrency must be at least 1", concurrency)
			return
		}
		o.concurrency = concurrency
	}
}

// WithOnError registers a callback that terminal operations invoke for every error they
// handle: errors marked ErrSuppressed just before skipping them, and the fatal error just
// before returning it. It is the hook for logging or metrics; it cannot alter the outcome.
// A nil callback is invalid: the stream it is applied to emits a single error and ends.
func WithOnError(fn func(error)) Option {
	return pipelineOption(func(o *options) {
		if fn == nil {
			o.err = errors.New("chunkflow: WithOnError: nil callback")
			return
		}
		o.onError = fn
	})
}

// IoBuilder binds a context and default options before the element type is known.
// Obtain one with NewIo and turn it into an IoStream with Seq, Seq2 or Chan; each of
// those is a generic method, so the element type is inferred from the source.
type IoBuilder struct {
	options options
	ctx     context.Context
}

// NewIo starts building an IoStream bound to ctx. The options become the defaults of
// every operation in the resulting pipeline (see Opts):
//
//	chunkflow.NewIo(ctx, chunkflow.WithParallel(8)).Chan(jobs).MapCtx(process).Exec()
func NewIo(ctx context.Context, opts ...Option) IoBuilder {
	o := defaultOptions()
	for _, opt := range opts {
		opt.applyPipeline(&o)
	}
	return IoBuilder{options: o, ctx: ctx}
}

// Seq wraps a native Go iterator. The context is checked before every element is
// emitted; once it is cancelled the next element carries the context error.
func (b IoBuilder) Seq[T any](seq iter.Seq[T]) IoStream[T] {
	return b.stream(func(yield func(result[T]) bool) {
		for item := range seq {
			if !yield(result[T]{value: item, err: b.ctx.Err()}) {
				return
			}
		}
	})
}

// Seq2 wraps a native (value, error) iterator, the inverse of IoStream.Seq. Elements
// whose error is nil still pick up the context error once the context is cancelled.
func (b IoBuilder) Seq2[T any](seq iter.Seq2[T, error]) IoStream[T] {
	return b.stream(func(yield func(result[T]) bool) {
		for item, err := range seq {
			if err == nil {
				err = b.ctx.Err()
			}
			if !yield(result[T]{value: item, err: err}) {
				return
			}
		}
	})
}

// Chan reads from ch until it is closed or the context is cancelled; a cancellation
// ends the stream with the context error as its last element.
//
// The resulting stream is single-use: a channel cannot be rewound, so iterating the
// stream twice yields whatever the first pass left behind. Stopping early (Take, First,
// a break) stops reading but does not close ch or signal the producer; a producer that
// may outlive the consumer must watch the same context.
func (b IoBuilder) Chan[T any](ch <-chan T) IoStream[T] {
	return b.stream(func(yield func(result[T]) bool) {
		for {
			select {
			case <-b.ctx.Done():
				yield(result[T]{err: b.ctx.Err()})
				return
			case item, ok := <-ch:
				if !ok {
					return
				}
				if !yield(result[T]{value: item}) {
					return
				}
			}
		}
	})
}

// Opts returns a copy of the stream with the given options applied as defaults
// for all subsequent operations.
func (s IoStream[T]) Opts(opts ...Option) IoStream[T] {
	for _, opt := range opts {
		opt.applyPipeline(&s.options)
	}
	if s.options.err != nil {
		return s.errStream[T](s.options.err)
	}
	return s
}

// Map transforms each element of the stream using the provided function.
func (s IoStream[T]) Map[R any](mapFn func(T) R) IoStream[R] {
	return s.MapCtx(func(_ context.Context, item T) (R, error) {
		return mapFn(item), nil
	}, WithParallel(1))
}

// MapCtx transforms each element using a context-aware function that may fail.
// With WithParallel(n > 1) elements are processed by n workers and the output order
// is not guaranteed. The first error terminates the stream.
func (s IoStream[T]) MapCtx[R any](mapFn func(ctx context.Context, item T) (R, error), opts ...StepOption) IoStream[R] {
	o := s.getOptions(opts...)
	if o.err != nil {
		return s.errStream[R](o.err)
	}

	if o.concurrency > 1 {
		return s.derive(s.mapCtxConcurrent(mapFn, o.concurrency))
	}

	return s.derive(func(yield func(result[R]) bool) {
		for item := range s.seq {
			if item.err != nil {
				if !yield(result[R]{err: item.err}) {
					return
				}
				continue
			}
			val, err := mapFn(s.ctx, item.value)
			if !yield(result[R]{value: val, err: err}) {
				return
			}
		}
	})
}

// Tap invokes fn for every value and passes it through unchanged. Errors flow past
// untouched; fn never sees them. Meant for side effects such as logging or metrics.
func (s IoStream[T]) Tap(fn func(T)) IoStream[T] {
	return s.TapCtx(func(_ context.Context, item T) error {
		fn(item)
		return nil
	}, WithParallel(1))
}

// TapCtx invokes a context-aware fn for every value and passes the value through
// unchanged. An error returned by fn replaces the value with that error in the
// stream. With WithParallel(n > 1) fn runs on n workers and the output order is not
// guaranteed, exactly as for MapCtx.
func (s IoStream[T]) TapCtx(fn func(ctx context.Context, item T) error, opts ...StepOption) IoStream[T] {
	return s.MapCtx(func(ctx context.Context, item T) (T, error) {
		return item, fn(ctx, item)
	}, opts...)
}

// Filter emits only the elements for which the predicate returns true.
func (s IoStream[T]) Filter(predicate func(T) bool) IoStream[T] {
	return s.FilterCtx(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	}, WithParallel(1))
}

// FilterCtx emits only the elements for which the context-aware predicate returns true.
// With WithParallel(n > 1) predicates are evaluated by n workers and the output order
// is not guaranteed. The first error terminates the stream.
func (s IoStream[T]) FilterCtx(predicate func(ctx context.Context, item T) (bool, error), opts ...StepOption) IoStream[T] {
	o := s.getOptions(opts...)
	if o.err != nil {
		return s.errStream[T](o.err)
	}

	if o.concurrency == 1 {
		return s.derive(func(yield func(result[T]) bool) {
			for item := range s.seq {
				if item.err != nil {
					if !yield(item) {
						return
					}
					continue
				}
				match, err := predicate(s.ctx, item.value)
				if err != nil {
					if !yield(result[T]{err: err}) {
						return
					}
					continue
				}
				if match && !yield(item) {
					return
				}
			}
		})
	}

	type filtered struct {
		val   T
		match bool
	}

	// Evaluate the predicate on the worker pool, then drop the non-matching values.
	// The pool yields a bare sequence rather than an IoStream[filtered]: instantiating
	// IoStream with a local type that depends on T would form a generic instantiation cycle.
	inner := s.mapCtxConcurrent(func(ctx context.Context, item T) (filtered, error) {
		match, err := predicate(ctx, item)
		return filtered{val: item, match: match}, err
	}, o.concurrency)

	return s.derive(func(yield func(result[T]) bool) {
		for res := range inner {
			if res.err != nil {
				if !yield(result[T]{err: res.err}) {
					return
				}
				continue
			}
			if res.value.match && !yield(result[T]{value: res.value.val}) {
				return
			}
		}
	})
}

// Take consumes at most nr elements from the stream and then short-circuits.
// Errors are passed through and do not count towards nr.
func (s IoStream[T]) Take(nr int) IoStream[T] {
	return s.derive(func(yield func(result[T]) bool) {
		if nr <= 0 {
			return
		}
		count := 0
		for item := range s.seq {
			if !yield(item) {
				return
			}
			if item.err != nil {
				continue
			}
			count++
			if count == nr {
				return
			}
		}
	})
}

// Skip bypasses the first nr elements and emits the remainder of the stream.
// Errors are passed through and do not count towards nr.
func (s IoStream[T]) Skip(nr int) IoStream[T] {
	return s.derive(func(yield func(result[T]) bool) {
		skipped := 0
		for item := range s.seq {
			if item.err != nil {
				if !yield(item) {
					return
				}
				continue
			}
			if skipped < nr {
				skipped++
				continue
			}
			if !yield(item) {
				return
			}
		}
	})
}

// TakeWhile emits values as long as the predicate returns true and short-circuits at
// the first value for which it returns false; nothing further is pulled from the source.
// Errors are passed through and are not evaluated by the predicate.
func (s IoStream[T]) TakeWhile(predicate func(T) bool) IoStream[T] {
	return s.TakeWhileCtx(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// TakeWhileCtx is TakeWhile with a context-aware predicate. A predicate error is
// emitted as an error element; the stream continues and the predicate keeps being
// evaluated on subsequent values, so a downstream CircuitBreaker can tolerate it.
func (s IoStream[T]) TakeWhileCtx(predicate func(ctx context.Context, item T) (bool, error)) IoStream[T] {
	return s.derive(func(yield func(result[T]) bool) {
		for item := range s.seq {
			if item.err != nil {
				if !yield(item) {
					return
				}
				continue
			}
			ok, err := predicate(s.ctx, item.value)
			if err != nil {
				if !yield(result[T]{err: err}) {
					return
				}
				continue
			}
			if !ok || !yield(item) {
				return
			}
		}
	})
}

// SkipWhile drops values as long as the predicate returns true and then emits every
// remaining element without evaluating the predicate again.
// Errors are passed through and are not evaluated by the predicate.
func (s IoStream[T]) SkipWhile(predicate func(T) bool) IoStream[T] {
	return s.SkipWhileCtx(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// SkipWhileCtx is SkipWhile with a context-aware predicate. A predicate error is
// emitted as an error element; the stream continues and the predicate keeps being
// evaluated on subsequent values.
func (s IoStream[T]) SkipWhileCtx(predicate func(ctx context.Context, item T) (bool, error)) IoStream[T] {
	return s.derive(func(yield func(result[T]) bool) {
		skipping := true
		for item := range s.seq {
			if item.err != nil {
				if !yield(item) {
					return
				}
				continue
			}
			if skipping {
				skip, err := predicate(s.ctx, item.value)
				if err != nil {
					if !yield(result[T]{err: err}) {
						return
					}
					continue
				}
				if skip {
					continue
				}
				skipping = false
			}
			if !yield(item) {
				return
			}
		}
	})
}

// CompactFunc drops consecutive duplicate values: a value is emitted only if eq reports
// it different from the previously emitted value. Like slices.CompactFunc it removes
// duplicates only when they are adjacent, so the input must be sorted or grouped for a
// global de-duplication. Memory is O(1). Errors pass through and do not reset the
// comparison: a value, an error, then the same value yields the value once.
func (s IoStream[T]) CompactFunc(eq func(a, b T) bool) IoStream[T] {
	return s.derive(func(yield func(result[T]) bool) {
		var last T
		first := true
		for item := range s.seq {
			if item.err == nil {
				if !first && eq(last, item.value) {
					continue
				}
				first = false
				last = item.value
			}
			if !yield(item) {
				return
			}
		}
	})
}

// Chunk groups elements into slices of the given size.
// The final chunk may contain fewer elements than size. Emits an error if size < 1.
// Errors are passed through immediately; the partially filled chunk is kept and
// continues to accumulate subsequent elements.
func (s IoStream[T]) Chunk[R []T](size int) IoStream[R] {
	if size < 1 {
		return s.errStream[R](fmt.Errorf("chunk size must be >= 1"))
	}

	return s.derive(func(yield func(result[R]) bool) {
		chunk := make([]T, 0, size)
		for item := range s.seq {
			if item.err != nil {
				if !yield(result[R]{err: item.err}) {
					return
				}
				continue
			}
			chunk = append(chunk, item.value)
			if len(chunk) == size {
				if !yield(result[R]{value: chunk}) {
					return
				}
				chunk = make([]T, 0, size)
			}
		}
		if len(chunk) > 0 {
			yield(result[R]{value: chunk})
		}
	})
}

// Through pipes the current stream into an external transformation function.
// It acts as a structural bridge to maintain fluent API chaining for operations that
// must be implemented as top-level functions (like IoFlatten).
func (s IoStream[T]) Through[R any](transform func(IoStream[T]) IoStream[R]) IoStream[R] {
	return transform(s)
}

// CircuitBreaker tolerates up to maxConsecutiveFailures-1 errors in a row and trips on
// the next one, interrupting the stream with a wrapped error. A successful element resets
// the counter.
//
// Tolerated errors are not dropped: they are re-emitted wrapped in an error matching
// ErrSuppressed so that downstream terminal operations skip them while consumers of
// Seq() can still observe them. Errors already marked as suppressed by an upstream
// breaker pass through without affecting the counter. Context errors (context.Canceled,
// context.DeadlineExceeded) are never suppressed.
//
// After a parallel stage the notion of "consecutive" follows arrival order, not source order.
func (s IoStream[T]) CircuitBreaker(maxConsecutiveFailures int) IoStream[T] {
	if maxConsecutiveFailures < 1 {
		return s.errStream[T](fmt.Errorf("threshold must be >= 1"))
	}

	return s.derive(func(yield func(result[T]) bool) {
		consecutiveFailures := 0
		for item := range s.seq {
			switch {
			case item.err == nil:
				consecutiveFailures = 0
			case isSuppressed(item.err):
				// already handled by an upstream breaker; not ours to count
			case errors.Is(item.err, context.Canceled) || errors.Is(item.err, context.DeadlineExceeded):
				yield(item)
				return
			default:
				consecutiveFailures++
				if consecutiveFailures >= maxConsecutiveFailures {
					yield(result[T]{
						err: fmt.Errorf("circuit breaker tripped after %d consecutive errors: %w",
							consecutiveFailures, item.err),
					})
					return
				}
				item = result[T]{err: &suppressedError{
					err:                 item.err,
					consecutiveFailures: consecutiveFailures,
					threshold:           maxConsecutiveFailures,
				}}
			}

			if !yield(item) {
				return
			}
		}
	})
}

// Seq returns the stream as a native (value, error) iterator. Elements tolerated by a
// CircuitBreaker are yielded with an error matching ErrSuppressed; iteration stops after
// the first error that is not suppressed.
func (s IoStream[T]) Seq() iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for item := range s.seq {
			if !yield(item.value, item.err) {
				return
			}
			if item.err != nil && !isSuppressed(item.err) {
				return
			}
		}
	}
}

// Collect materializes the stream into a slice. Elements collected before an error
// are returned together with that error.
func (s IoStream[T]) Collect() ([]T, error) {
	var res []T
	err := s.ForEach(func(item T) {
		res = append(res, item)
	})
	return res, err
}

// ForEach executes a side effect for each element and returns the first error.
func (s IoStream[T]) ForEach(fn func(T)) error {
	return s.ForEachCtx(func(_ context.Context, item T) error {
		fn(item)
		return nil
	})
}

// ForEachCtx executes a context-aware side effect for each element and returns the first error.
func (s IoStream[T]) ForEachCtx(fn func(ctx context.Context, item T) error) error {
	return s.each(func(item T) (bool, error) {
		return true, fn(s.ctx, item)
	})
}

// Count consumes the entire stream and returns the number of elements seen before the first error.
func (s IoStream[T]) Count() (int, error) {
	n := 0
	err := s.each(func(T) (bool, error) {
		n++
		return true, nil
	})
	return n, err
}

// Exec exhausts the stream, discarding values, and returns the first error.
func (s IoStream[T]) Exec() error {
	return s.each(func(T) (bool, error) { return true, nil })
}

// Reduce folds the stream into a single value: starting from init, fn is called as
// fn(acc, item) for every value and its result becomes the next accumulator. The
// accumulator type R is independent of the element type. On error the accumulator
// built so far is returned together with the error.
func (s IoStream[T]) Reduce[R any](init R, fn func(acc R, item T) R) (R, error) {
	return s.ReduceCtx(init, func(_ context.Context, acc R, item T) (R, error) {
		return fn(acc, item), nil
	})
}

// ReduceCtx is Reduce with a context-aware fn that may fail; a returned error stops the
// fold and is returned with the accumulator built so far. A fold is sequential by nature,
// so unlike MapCtx it takes no StepOption.
func (s IoStream[T]) ReduceCtx[R any](init R, fn func(ctx context.Context, acc R, item T) (R, error)) (R, error) {
	acc := init
	err := s.each(func(item T) (bool, error) {
		var err error
		acc, err = fn(s.ctx, acc, item)
		return true, err
	})
	return acc, err
}

// All verifies whether all elements satisfy the predicate. Short-circuits on the first mismatch.
//
// On an empty stream, or one consisting only of suppressed errors, All returns true
// (vacuous truth): no element could violate the predicate. All(p) == !Any(!p) always holds.
func (s IoStream[T]) All(predicate func(T) bool) (bool, error) {
	return s.AllCtx(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// AllCtx verifies whether all elements satisfy the context-aware predicate.
func (s IoStream[T]) AllCtx(predicate func(ctx context.Context, item T) (bool, error)) (bool, error) {
	all := true
	err := s.each(func(item T) (bool, error) {
		match, err := predicate(s.ctx, item)
		if err != nil {
			return false, err
		}
		all = match
		return match, nil
	})
	if err != nil {
		return false, err
	}
	return all, nil
}

// Any verifies whether at least one element satisfies the predicate. Short-circuits on the first match.
//
// On an empty stream, or one consisting only of suppressed errors, Any returns false:
// no element could be a witness.
func (s IoStream[T]) Any(predicate func(T) bool) (bool, error) {
	return s.AnyCtx(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// AnyCtx verifies whether at least one element satisfies the context-aware predicate.
func (s IoStream[T]) AnyCtx(predicate func(ctx context.Context, item T) (bool, error)) (bool, error) {
	found := false
	err := s.each(func(item T) (bool, error) {
		match, err := predicate(s.ctx, item)
		if err != nil {
			return false, err
		}
		found = match
		return !match, nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// First consumes at most one element. Returns (zero, false, nil) for an empty stream.
func (s IoStream[T]) First() (T, bool, error) {
	var first T
	ok := false
	err := s.each(func(item T) (bool, error) {
		first, ok = item, true
		return false, nil
	})
	if err != nil {
		var zero T
		return zero, false, err
	}
	return first, ok, nil
}

// Last consumes the entire stream and returns the final element.
func (s IoStream[T]) Last() (T, bool, error) {
	var last T
	ok := false
	err := s.each(func(item T) (bool, error) {
		last, ok = item, true
		return true, nil
	})
	return last, ok, err
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

// IoCompact drops consecutive duplicate values using ==. It is the IoStream counterpart
// of Compact; use it with Through. See IoStream.CompactFunc for the semantics.
func IoCompact[T comparable](stream IoStream[T]) IoStream[T] {
	return stream.CompactFunc(func(a, b T) bool { return a == b })
}

// IoConcat emits every element of the first stream, then of the second, and so on.
// Order is deterministic and a stream is not touched until all previous ones are
// exhausted. Errors pass through at their position. The result inherits the options of
// the first stream and a context that is cancelled when any source context is (see
// mergeContexts); with no arguments it returns an empty stream.
func IoConcat[T any](streams ...IoStream[T]) IoStream[T] {
	if len(streams) == 0 {
		return emptyIoStream[T]()
	}
	out := streams[0]
	out.ctx = mergeContexts(streams)
	return out.derive(func(yield func(result[T]) bool) {
		for _, s := range streams {
			for item := range s.seq {
				if !yield(item) {
					return
				}
			}
		}
	})
}

// IoMerge consumes all streams concurrently, one goroutine per source, and emits
// elements as they arrive. The interleaving is not deterministic. Errors pass through
// at their arrival position. When the consumer stops, or when the context of any
// source is cancelled, every source goroutine is released and the cancellation is
// reported as an error element carrying the original cause. The result inherits the
// options of the first stream and the merged context; with no arguments it returns an
// empty stream.
//
// There is no Stream counterpart: merging requires goroutines and a context to shut
// them down, which the synchronous Stream does not have.
func IoMerge[T any](streams ...IoStream[T]) IoStream[T] {
	if len(streams) == 0 {
		return emptyIoStream[T]()
	}
	head := streams[0]
	head.ctx = mergeContexts(streams)
	return head.derive(func(yield func(result[T]) bool) {
		ctx, cancel := context.WithCancel(head.ctx)
		defer cancel()

		out := make(chan result[T], len(streams))
		var wg sync.WaitGroup
		wg.Add(len(streams))
		for _, s := range streams {
			go func() {
				defer wg.Done()
				for item := range s.seq {
					select {
					case out <- item:
					case <-ctx.Done():
						return
					}
				}
			}()
		}
		go func() {
			wg.Wait()
			close(out)
		}()

		for item := range out {
			if !yield(item) {
				return
			}
		}
		if head.ctx.Err() != nil {
			yield(result[T]{err: context.Cause(head.ctx)})
		}
	})
}

// IoFlatten unwraps a stream of slices into a flat stream of individual elements.
// It is the IoStream counterpart of Flatten; use it with Through.
func IoFlatten[E any](stream IoStream[[]E]) IoStream[E] {
	return stream.derive(func(yield func(result[E]) bool) {
		for chunk := range stream.seq {
			if chunk.err != nil {
				if !yield(result[E]{err: chunk.err}) {
					return
				}
				continue
			}
			for _, val := range chunk.value {
				if !yield(result[E]{value: val}) {
					return
				}
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

// result is the internal element wrapper: either a value or an error.
type result[T any] struct {
	value T
	err   error
}

// options holds pipeline settings. err records an invalid option value; the first
// operation that consumes the options turns it into a stream emitting that error.
type options struct {
	concurrency int
	onError     func(error)
	err         error
}

func defaultOptions() options {
	return options{
		concurrency: 1,
		onError:     func(error) {},
	}
}

// mergeContexts returns a context that carries the values and deadline of the first
// stream's context and is cancelled as soon as any stream's context is cancelled, with
// the original cause preserved (see context.Cause). Identical contexts are registered
// once. Contexts are compared with ==, which holds for every context.Context produced
// by the standard library.
func mergeContexts[T any](streams []IoStream[T]) context.Context {
	head := streams[0].ctx
	seen := []context.Context{head}
	for _, s := range streams[1:] {
		if !slices.Contains(seen, s.ctx) {
			seen = append(seen, s.ctx)
		}
	}
	others := seen[1:]
	if len(others) == 0 {
		return head // every stream shares the head context; nothing to merge
	}

	merged, cancel := context.WithCancelCause(head)
	link := func(other context.Context) {
		context.AfterFunc(other, func() { cancel(context.Cause(other)) })
	}
	link(others[0])
	for _, other := range others[1:] {
		link(other)
	}
	return merged
}

// emptyIoStream returns a stream that ends immediately, for combinators called with no inputs.
func emptyIoStream[T any]() IoStream[T] {
	return IoStream[T]{
		options: defaultOptions(),
		ctx:     context.Background(),
		seq:     func(func(result[T]) bool) {},
	}
}

func (s IoStream[T]) getOptions(opts ...StepOption) options {
	o := s.options
	for _, apply := range opts {
		apply(&o)
	}
	return o
}

// derive builds a new stream of another element type that inherits ctx and options.
func (s IoStream[T]) derive[R any](seq iter.Seq[result[R]]) IoStream[R] {
	return IoStream[R]{options: s.options, ctx: s.ctx, seq: seq}
}

// errStream builds a stream that emits a single error and ends.
func (s IoStream[T]) errStream[R any](err error) IoStream[R] {
	return s.derive(func(yield func(result[R]) bool) {
		yield(result[R]{err: err})
	})
}

// each drives every terminal operation: it reports every error to the OnError hook,
// skips elements carrying a suppressed error, returns the first other error, and stops
// early when fn returns false.
func (s IoStream[T]) each(fn func(T) (bool, error)) error {
	for item := range s.seq {
		if item.err != nil {
			s.options.onError(item.err)
			if isSuppressed(item.err) {
				continue
			}
			return item.err
		}
		next, err := fn(item.value)
		if err != nil {
			s.options.onError(err)
			return err
		}
		if !next {
			return nil
		}
	}
	return nil
}

// stream assembles an IoStream from the builder's context and options. An invalid option
// given to NewIo surfaces here as a stream emitting that error.
func (b IoBuilder) stream[T any](seq iter.Seq[result[T]]) IoStream[T] {
	if b.options.err != nil {
		seq = func(yield func(result[T]) bool) { yield(result[T]{err: b.options.err}) }
	}
	return IoStream[T]{options: b.options, ctx: b.ctx, seq: seq}
}

// mapCtxConcurrent is the WithParallel(n > 1) path of MapCtx and FilterCtx,
// returned as a bare sequence so callers can wrap it in any element type. It wires three kinds
// of goroutines: a feeder that reads the upstream sequence into inChan (forwarding
// upstream errors straight to outChan), n workers that apply mapFn and push results to
// outChan, and a closer that closes outChan once every writer has exited. Every writer
// is tracked by the WaitGroup so the channel is never closed under a live sender.
// A consumer stopping early cancels ctx, which releases all of them.
func (s IoStream[T]) mapCtxConcurrent[R any](mapFn func(ctx context.Context, item T) (R, error), concurrency int) iter.Seq[result[R]] {
	return func(yield func(result[R]) bool) {
		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()

		inChan := make(chan T, concurrency)
		outChan := make(chan result[R], concurrency)

		var wg sync.WaitGroup
		wg.Add(1 + concurrency)

		// feeder
		go func() {
			defer wg.Done()
			defer close(inChan)
			for item := range s.seq {
				if item.err != nil {
					select {
					case outChan <- result[R]{err: item.err}:
					case <-ctx.Done():
						return
					}
					continue
				}
				select {
				case inChan <- item.value:
				case <-ctx.Done():
					return
				}
			}
		}()

		// workers
		for range concurrency {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case val, ok := <-inChan:
						if !ok {
							return
						}
						res, err := mapFn(ctx, val)
						select {
						case outChan <- result[R]{value: res, err: err}:
						case <-ctx.Done():
							return
						}
					}
				}
			}()
		}

		// closer
		go func() {
			wg.Wait()
			close(outChan)
		}()

		for res := range outChan {
			if !yield(res) {
				return
			}
		}
		// Writers bail out silently on ctx.Done(); make sure a cancellation of the
		// stream context still surfaces to the consumer as an error.
		if err := s.ctx.Err(); err != nil {
			yield(result[R]{err: err})
		}
	}
}
