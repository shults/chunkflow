package chunkflow

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"sync"
)

// IoStream represents a lazily evaluated, context-aware pipeline whose elements may
// carry errors. It mirrors the Stream API; every method that accepts a user callback
// has an *Async counterpart that receives a context and may return an error.
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

// result is the internal element wrapper: either a value or an error.
type result[T any] struct {
	value T
	err   error
}

type options struct {
	concurrency int
	logger      *slog.Logger
}

// Option configures an IoStream or a single asynchronous operation.
type Option func(*options)

// WithParallel sets the number of concurrent workers used by *Async operations.
func WithParallel(concurrency int) Option {
	return func(o *options) {
		o.concurrency = concurrency
	}
}

// WithLogger sets the logger used for diagnostics. Diagnostics are discarded by default.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

// WithDiscardLogger drops all diagnostics. Handy to silence a logger previously set via Opts.
func WithDiscardLogger() Option {
	return WithLogger(slog.New(slog.DiscardHandler))
}

func defaultOptions() options {
	return options{
		concurrency: 1,
		logger:      slog.New(slog.DiscardHandler),
	}
}

// NewIoStream wraps a native Go iterator into a context-aware IoStream.
// The context is checked before every element is emitted.
func NewIoStream[T any](ctx context.Context, seq iter.Seq[T]) IoStream[T] {
	return IoStream[T]{
		options: defaultOptions(),
		ctx:     ctx,
		seq: func(yield func(result[T]) bool) {
			for item := range seq {
				if !yield(result[T]{value: item, err: ctx.Err()}) {
					return
				}
			}
		},
	}
}

// NewIoStream2 wraps a native (value, error) iterator into an IoStream.
// It is the inverse of IoStream.Seq.
func NewIoStream2[T any](ctx context.Context, seq iter.Seq2[T, error]) IoStream[T] {
	return IoStream[T]{
		options: defaultOptions(),
		ctx:     ctx,
		seq: func(yield func(result[T]) bool) {
			for item, err := range seq {
				if err == nil {
					err = ctx.Err()
				}
				if !yield(result[T]{value: item, err: err}) {
					return
				}
			}
		},
	}
}

// Opts returns a copy of the stream with the given options applied as defaults
// for all subsequent operations.
func (s IoStream[T]) Opts(opts ...Option) IoStream[T] {
	s.options = s.getOptions(opts...)
	return s
}

func (s IoStream[T]) getOptions(opts ...Option) options {
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

// ---------------------------------------------------------------------------
// Intermediate operations
// ---------------------------------------------------------------------------

// Map transforms each element of the stream using the provided function.
func (s IoStream[T]) Map[R any](mapFn func(T) R) IoStream[R] {
	return s.MapAsync(func(_ context.Context, item T) (R, error) {
		return mapFn(item), nil
	}, WithParallel(1))
}

// MapAsync transforms each element using a context-aware function that may fail.
// With WithParallel(n > 1) elements are processed by n workers and the output order
// is not guaranteed. The first error terminates the stream.
func (s IoStream[T]) MapAsync[R any](mapFn func(ctx context.Context, item T) (R, error), opts ...Option) IoStream[R] {
	o := s.getOptions(opts...)
	if o.concurrency < 1 {
		return s.errStream[R](fmt.Errorf("concurrency must be at least 1"))
	}

	if o.concurrency == 1 {
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

	return s.derive(func(yield func(result[R]) bool) {
		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()

		outChan := make(chan result[R], o.concurrency)
		runConcurrent(ctx, s.seq, o.concurrency, mapFn, outChan)

		for res := range outChan {
			if !yield(res) {
				return
			}
		}
		// Workers and the feeder bail out silently on ctx.Done(); make sure the
		// cancellation still surfaces to the consumer as an error.
		if err := s.ctx.Err(); err != nil {
			yield(result[R]{err: err})
		}
	})
}

// Filter emits only the elements for which the predicate returns true.
func (s IoStream[T]) Filter(predicate func(T) bool) IoStream[T] {
	return s.FilterAsync(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	}, WithParallel(1))
}

// FilterAsync emits only the elements for which the context-aware predicate returns true.
// With WithParallel(n > 1) predicates are evaluated by n workers and the output order
// is not guaranteed. The first error terminates the stream.
func (s IoStream[T]) FilterAsync(predicate func(ctx context.Context, item T) (bool, error), opts ...Option) IoStream[T] {
	o := s.getOptions(opts...)
	if o.concurrency < 1 {
		return s.errStream[T](fmt.Errorf("concurrency must be at least 1"))
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

	return s.derive(func(yield func(result[T]) bool) {
		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()

		outChan := make(chan result[filtered], o.concurrency)
		runConcurrent(ctx, s.seq, o.concurrency, func(ctx context.Context, item T) (filtered, error) {
			match, err := predicate(ctx, item)
			return filtered{val: item, match: match}, err
		}, outChan)

		for res := range outChan {
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
		if err := s.ctx.Err(); err != nil {
			yield(result[T]{err: err})
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
// Terminal operations
// ---------------------------------------------------------------------------

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

// each drives every terminal operation: it skips elements carrying a suppressed error,
// returns the first other error, and stops early when fn returns false.
func (s IoStream[T]) each(fn func(T) (bool, error)) error {
	for item := range s.seq {
		if item.err != nil {
			if isSuppressed(item.err) {
				continue
			}
			return item.err
		}
		next, err := fn(item.value)
		if err != nil {
			return err
		}
		if !next {
			return nil
		}
	}
	return nil
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
	return s.ForEachAsync(func(_ context.Context, item T) error {
		fn(item)
		return nil
	})
}

// ForEachAsync executes a context-aware side effect for each element and returns the first error.
func (s IoStream[T]) ForEachAsync(fn func(ctx context.Context, item T) error) error {
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

// Reduce aggregates the stream into a single value.
func (s IoStream[T]) Reduce(init T, fn func(item, acc T) T) (T, error) {
	return s.ReduceAsync(init, func(_ context.Context, item, acc T) (T, error) {
		return fn(item, acc), nil
	})
}

// ReduceAsync aggregates the stream into a single value using a context-aware function.
// Reduction is always sequential; WithParallel is accepted for API symmetry and logged.
func (s IoStream[T]) ReduceAsync(init T, fn func(ctx context.Context, item, acc T) (T, error), opts ...Option) (T, error) {
	o := s.getOptions(opts...)
	if o.concurrency > 1 {
		o.logger.Warn("ReduceAsync called with concurrency > 1; concurrent reduction is not supported, falling back to sequential reduction")
	}

	acc := init
	err := s.each(func(item T) (bool, error) {
		var err error
		acc, err = fn(s.ctx, item, acc)
		return true, err
	})
	return acc, err
}

// All verifies whether all elements satisfy the predicate. Short-circuits on the first mismatch.
//
// On an empty stream, or one consisting only of suppressed errors, All returns true
// (vacuous truth): no element could violate the predicate. All(p) == !Any(!p) always holds.
func (s IoStream[T]) All(predicate func(T) bool) (bool, error) {
	return s.AllAsync(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// AllAsync verifies whether all elements satisfy the context-aware predicate.
func (s IoStream[T]) AllAsync(predicate func(ctx context.Context, item T) (bool, error)) (bool, error) {
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
	return s.AnyAsync(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// AnyAsync verifies whether at least one element satisfies the context-aware predicate.
func (s IoStream[T]) AnyAsync(predicate func(ctx context.Context, item T) (bool, error)) (bool, error) {
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
// Concurrency plumbing
// ---------------------------------------------------------------------------

// runConcurrent wires a feeder goroutine, a worker pool and a closer:
// the feeder reads inputSeq into inChan (forwarding upstream errors straight to
// outputChan), workers apply workerFn and push to outputChan, and the closer closes
// outputChan once all workers exit.
func runConcurrent[T any, R any](
	ctx context.Context,
	inputSeq iter.Seq[result[T]],
	concurrency int,
	workerFn func(ctx context.Context, item T) (R, error),
	outputChan chan<- result[R],
) {
	inChan := make(chan T, concurrency)

	// Every goroutine that may write to outputChan (feeder + workers) is tracked by wg,
	// so the closer never closes the channel while a writer is still alive.
	var wg sync.WaitGroup
	wg.Add(1 + concurrency)

	go func() {
		defer wg.Done()
		defer close(inChan)
		for item := range inputSeq {
			if item.err != nil {
				select {
				case outputChan <- result[R]{err: item.err}:
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
					res, err := workerFn(ctx, val)
					select {
					case outputChan <- result[R]{value: res, err: err}:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outputChan)
	}()
}
