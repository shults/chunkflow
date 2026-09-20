package chunkflow

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"time"
)

// Stream is a lazily evaluated, context-aware pipeline over a native iterator whose
// elements may carry errors. Nothing runs until a terminal operation pulls. Every method
// that accepts a user callback has an *Ctx variant whose callback receives a context and
// may return an error; the plain variant is that *Ctx variant pinned to one worker.
//
// Error semantics: an error produced by the source or by a callback travels down the
// pipeline as an element. Intermediate operations pass it through untouched and keep
// processing the remaining input; they operate on values only and never interpret
// errors. A policy such as policy.CircuitBreaker, or a callback returning Suppress(err),
// may mark an error as tolerated (see ErrSuppressed). Terminal operations skip elements whose error matches
// ErrSuppressed and stop at the first other error, so a pipeline that tolerates nothing
// behaves as "fail fast".
//
// A panic inside any user callback is recovered where it happens, also on worker
// goroutines, and becomes an error matching ErrPanic that no operator may suppress.
// Whether a step runs sequentially or on WithParallel(n) workers therefore makes no
// difference to how a bug surfaces: the terminal returns it.
type Stream[T any] struct {
	options options
	ctx     context.Context
	seq     iter.Seq[result[T]]
}

// Option configures a whole pipeline. Pass it to Opts; every operation downstream
// inherits it. Every StepOption is also an Option.
type Option interface {
	applyPipeline(*options)
}

// StepOption configures a single *Ctx call, e.g. MapCtx(fn, WithParallel(8)). Passing it
// to Opts instead makes it the default for everything downstream.
type StepOption func(*options)

func (f StepOption) applyPipeline(o *options) { f(o) }

// pipelineOption is an Option that makes no sense for a single step.
type pipelineOption func(*options)

func (f pipelineOption) applyPipeline(o *options) { f(o) }

// WithParallel sets the number of concurrent workers used by MapCtx, FilterCtx and TapCtx.
// Results are emitted in source order regardless of n, so a parallel step is
// indistinguishable from a sequential one except for speed; see WithUnordered to trade
// that for throughput. A value below 1 is invalid: the stream it is applied to emits a
// single error and ends.
//
// Ordering has a price. A finished item waits for its slower predecessors, and the
// step reads ahead of the slowest one by a bounded window (currently 2n items pulled
// from the source and not yet emitted); once the window is full, idle workers wait
// rather than pull. One very slow item therefore stalls the whole step behind it,
// with memory bounded by the window.
func WithParallel(concurrency int) StepOption {
	return func(o *options) {
		if concurrency < 1 {
			o.err = fmt.Errorf("chunkflow: WithParallel(%d): concurrency must be at least 1", concurrency)
			return
		}
		o.concurrency = concurrency
	}
}

// WithUnordered lets a parallel step (WithParallel(n > 1)) emit results as workers
// finish them instead of in source order. Use it when the consumer does not care about
// order, typically side-effect pipelines ending in Drain, to avoid head-of-line blocking:
// no result ever waits for a slower predecessor. A downstream policy.CircuitBreaker then
// counts consecutive errors in arrival order. It has no effect on a single worker.
func WithUnordered() StepOption {
	return func(o *options) {
		o.unordered = true
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

// Builder binds a context before the element type is known. Obtain one with New and
// turn it into a Stream with Seq, Seq2, SeqErr or Chan; each of those is a generic method,
// so the element type is inferred from the source.
type Builder struct {
	ctx context.Context
}

// New starts building a Stream bound to ctx. Pipeline options are set on the stream
// with Opts, step options on the individual *Ctx call:
//
//	chunkflow.New(ctx).Chan(jobs).Opts(chunkflow.WithParallel(8)).MapCtx(process).Drain()
func New(ctx context.Context) Builder {
	return Builder{ctx: ctx}
}

// Seq wraps a native Go iterator. The context is checked before every element is
// emitted; once it is cancelled the next element carries the context error.
func (b Builder) Seq[T any](seq iter.Seq[T]) Stream[T] {
	return b.stream(func(yield func(result[T]) bool) {
		for item := range seq {
			if !yield(result[T]{value: item, err: b.ctx.Err()}) {
				return
			}
		}
	})
}

// Seq2 wraps a native two-value iterator, such as maps.All or slices.All, as a stream of
// Entry values: the first value becomes Key, the second Value. For an iterator whose second
// value is an error, use SeqErr, which turns it into the stream's own error channel instead.
// The context is checked before every element, as in Seq.
func (b Builder) Seq2[K, V any](seq iter.Seq2[K, V]) Stream[Entry[K, V]] {
	return b.stream(func(yield func(result[Entry[K, V]]) bool) {
		for k, v := range seq {
			if !yield(result[Entry[K, V]]{value: Entry[K, V]{Key: k, Value: v}, err: b.ctx.Err()}) {
				return
			}
		}
	})
}

// Entry is one element of a stream built with Seq2: a key/value pair as yielded by maps.All,
// slices.All (index and element) or any other iter.Seq2[K, V]. It is plain data with no
// behaviour, the one exported pair in the package: a map entry has exactly two named sides
// and never nests, unlike the pairs a Zip would build, which is why Zip takes a callback
// instead.
type Entry[K, V any] struct {
	Key   K
	Value V
}

// SeqErr wraps a native (value, error) iterator, the inverse of Stream.Seq: the error of each
// pair becomes the element's error, so a failing source ends the pipeline exactly like a
// failing callback. Elements whose error is nil still pick up the context error once the
// context is cancelled.
func (b Builder) SeqErr[T any](seq iter.Seq2[T, error]) Stream[T] {
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
func (b Builder) Chan[T any](ch <-chan T) Stream[T] {
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
func (s Stream[T]) Opts(opts ...Option) Stream[T] {
	for _, opt := range opts {
		opt.applyPipeline(&s.options)
	}
	if s.options.err != nil {
		return s.errStream[T](s.options.err)
	}
	return s
}

// Map transforms each element of the stream using the provided function.
func (s Stream[T]) Map[R any](mapFn func(T) R) Stream[R] {
	return s.MapCtx(func(_ context.Context, item T) (R, error) {
		return mapFn(item), nil
	}, WithParallel(1))
}

// MapCtx transforms each element using a context-aware function that may fail.
// With WithParallel(n > 1) elements are processed by n workers; results, errors included,
// still come out in source order unless WithUnordered is given (see WithParallel for the
// cost of ordering). The first error terminates the stream.
//
// Shutdown is not instantaneous: when a terminal stops because of a fatal error (or
// a consumer breaks out of Seq), workers that are already inside mapFn finish that
// call, and items pulled from the source into the pool's buffer may still be handed
// to mapFn before the pool notices the cancellation. Up to n calls may therefore run
// after the terminal has returned; their results are discarded. A callback with side
// effects must tolerate this, or check ctx before acting.
func (s Stream[T]) MapCtx[R any](mapFn func(ctx context.Context, item T) (R, error), opts ...StepOption) Stream[R] {
	o := s.getOptions(opts...)
	if o.err != nil {
		return s.errStream[R](o.err)
	}

	if o.concurrency > 1 {
		return s.derive(s.mapCtxConcurrent(mapFn, o))
	}

	return s.derive(func(yield func(result[R]) bool) {
		var inCallback bool
		defer guardPanics(&inCallback, func(err error) { yield(result[R]{err: err}) })
		for item := range s.seq {
			if item.err != nil {
				if !yield(result[R]{err: item.err}) {
					return
				}
				continue
			}
			inCallback = true
			val, err := mapFn(s.ctx, item.value)
			inCallback = false
			if !yield(result[R]{value: val, err: err}) {
				return
			}
		}
	})
}

// Tap invokes fn for every value and passes it through unchanged. Errors flow past
// untouched; fn never sees them. Meant for side effects such as logging or metrics.
func (s Stream[T]) Tap(fn func(T)) Stream[T] {
	return s.TapCtx(func(_ context.Context, item T) error {
		fn(item)
		return nil
	}, WithParallel(1))
}

// TapCtx invokes a context-aware fn for every value and passes the value through
// unchanged. An error returned by fn replaces the value with that error in the
// stream. With WithParallel(n > 1) fn runs on n workers with the ordering and shutdown
// behaviour documented on MapCtx; the latter matters most here, because TapCtx exists for
// side effects. WithUnordered is the natural companion when the order of those side
// effects is irrelevant.
func (s Stream[T]) TapCtx(fn func(ctx context.Context, item T) error, opts ...StepOption) Stream[T] {
	return s.MapCtx(func(ctx context.Context, item T) (T, error) {
		return item, fn(ctx, item)
	}, opts...)
}

// Filter emits only the elements for which the predicate returns true.
func (s Stream[T]) Filter(predicate func(T) bool) Stream[T] {
	return s.FilterCtx(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	}, WithParallel(1))
}

// FilterCtx emits only the elements for which the context-aware predicate returns true.
// With WithParallel(n > 1) predicates are evaluated by n workers, in source order unless
// WithUnordered is given, and after a fatal error the pool may still evaluate the
// predicate on buffered items before it shuts down (see MapCtx). The first error
// terminates the stream.
func (s Stream[T]) FilterCtx(predicate func(ctx context.Context, item T) (bool, error), opts ...StepOption) Stream[T] {
	o := s.getOptions(opts...)
	if o.err != nil {
		return s.errStream[T](o.err)
	}

	if o.concurrency == 1 {
		return s.derive(func(yield func(result[T]) bool) {
			var inCallback bool
			defer guardPanics(&inCallback, func(err error) { yield(result[T]{err: err}) })
			for item := range s.seq {
				if item.err != nil {
					if !yield(item) {
						return
					}
					continue
				}
				inCallback = true
				match, err := predicate(s.ctx, item.value)
				inCallback = false
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
	// The pool yields a bare sequence rather than an Stream[filtered]: instantiating
	// Stream with a local type that depends on T would form a generic instantiation cycle.
	inner := s.mapCtxConcurrent(func(ctx context.Context, item T) (filtered, error) {
		match, err := predicate(ctx, item)
		return filtered{val: item, match: match}, err
	}, o)

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
func (s Stream[T]) Take(nr int) Stream[T] {
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
func (s Stream[T]) Skip(nr int) Stream[T] {
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
func (s Stream[T]) TakeWhile(predicate func(T) bool) Stream[T] {
	return s.TakeWhileCtx(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// TakeWhileCtx is TakeWhile with a context-aware predicate. A predicate error is
// emitted as an error element; the stream continues and the predicate keeps being
// evaluated on subsequent values, so a downstream policy can tolerate it.
func (s Stream[T]) TakeWhileCtx(predicate func(ctx context.Context, item T) (bool, error)) Stream[T] {
	return s.derive(func(yield func(result[T]) bool) {
		var inCallback bool
		defer guardPanics(&inCallback, func(err error) { yield(result[T]{err: err}) })
		for item := range s.seq {
			if item.err != nil {
				if !yield(item) {
					return
				}
				continue
			}
			inCallback = true
			ok, err := predicate(s.ctx, item.value)
			inCallback = false
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
func (s Stream[T]) SkipWhile(predicate func(T) bool) Stream[T] {
	return s.SkipWhileCtx(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// SkipWhileCtx is SkipWhile with a context-aware predicate. A predicate error is
// emitted as an error element; the stream continues and the predicate keeps being
// evaluated on subsequent values.
func (s Stream[T]) SkipWhileCtx(predicate func(ctx context.Context, item T) (bool, error)) Stream[T] {
	return s.derive(func(yield func(result[T]) bool) {
		var inCallback bool
		defer guardPanics(&inCallback, func(err error) { yield(result[T]{err: err}) })
		skipping := true
		for item := range s.seq {
			if item.err != nil {
				if !yield(item) {
					return
				}
				continue
			}
			if skipping {
				inCallback = true
				skip, err := predicate(s.ctx, item.value)
				inCallback = false
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
func (s Stream[T]) CompactFunc(eq func(a, b T) bool) Stream[T] {
	return s.derive(func(yield func(result[T]) bool) {
		var inCallback bool
		defer guardPanics(&inCallback, func(err error) { yield(result[T]{err: err}) })
		var last T
		first := true
		for item := range s.seq {
			if item.err == nil {
				if !first {
					inCallback = true
					same := eq(last, item.value)
					inCallback = false
					if same {
						continue
					}
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
func (s Stream[T]) Chunk[R []T](size int) Stream[R] {
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

// ChunkTimeout is Chunk with a bound on how long a value may wait for its chunk: a chunk is
// emitted when it reaches size or when maxWait has passed since its first value arrived,
// whichever comes first. The clock starts with the first value of a chunk, so an idle source
// never produces empty chunks. It is the batching operator for sources that trickle, such as
// a channel fed by producers, where Chunk would hold a partial batch until the source speaks
// again.
//
// Unlike Chunk it needs a goroutine: a pull-based iterator only gets control when the source
// yields, so the source is read on a feeder goroutine and the emitter selects between the next
// value and the timer. That costs a channel handoff per value, and, as with every operator that
// reads the source from a goroutine, a consumer that stops early cannot interrupt a source that
// is blocked inside its own pull; a Chan source blocked on its channel is released when the
// channel or its context gives up, not by Take. Errors pass through immediately, the partial
// chunk is kept and its clock keeps running. The stream's cancellation surfaces as an error
// even when the feeder exits without emitting. size below 1 or a non-positive maxWait emits a
// single error and ends.
func (s Stream[T]) ChunkTimeout[R []T](size int, maxWait time.Duration) Stream[R] {
	if size < 1 {
		return s.errStream[R](fmt.Errorf("chunkflow: ChunkTimeout(%d, %v): size must be at least 1", size, maxWait))
	}
	if maxWait <= 0 {
		return s.errStream[R](fmt.Errorf("chunkflow: ChunkTimeout(%d, %v): maxWait must be positive", size, maxWait))
	}

	return s.derive(func(yield func(result[R]) bool) {
		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()

		items := make(chan result[T], size)
		go func() { // feeder
			defer close(items)
			for item := range s.seq {
				select {
				case items <- item:
				case <-ctx.Done():
					return
				}
			}
		}()

		chunk := make([]T, 0, size)
		var timer *time.Timer         // armed while a chunk is open
		var deadline <-chan time.Time // nil while no chunk is open: a nil channel never fires
		defer func() {
			if timer != nil {
				timer.Stop()
			}
		}()

		// flush emits the open chunk, if any, and disarms its clock. It never emits an empty
		// chunk, whoever calls it; the timer is only ever armed by a value, so the deadline
		// branch below always finds one, but the guard makes that a fact to read, not to prove.
		flush := func() bool {
			if timer != nil {
				timer.Stop()
				timer, deadline = nil, nil
			}
			if len(chunk) == 0 {
				return true
			}
			out := chunk
			chunk = make([]T, 0, size)
			return yield(result[R]{value: out})
		}

		for {
			select {
			case item, ok := <-items:
				if !ok {
					if !flush() {
						return
					}
					// The feeder bails out silently on ctx.Done(); make sure a cancellation of the
					// stream context still surfaces to the consumer as an error.
					if err := s.ctx.Err(); err != nil {
						yield(result[R]{err: err})
					}
					return
				}
				if item.err != nil {
					if !yield(result[R]{err: item.err}) {
						return
					}
					continue
				}
				chunk = append(chunk, item.value)
				switch {
				case len(chunk) == size:
					if !flush() {
						return
					}
				case timer == nil:
					timer = time.NewTimer(maxWait)
					deadline = timer.C
				}
			case <-deadline:
				if !flush() {
					return
				}
			}
		}
	})
}

// Zip pairs the values of this stream with the values of other, position by position, and
// emits fn(a, b) for each pair. It ends when either side ends; a value already pulled from the
// longer side is dropped, as in the iter.Zip proposal for the standard library. Errors are not
// paired: an error on either side is forwarded at its position without consuming a value from
// the other side, so `Zip` obeys the same rule as every operator, values are what it works on.
//
// The result keeps this stream's options and a context merged like Merge does: values and
// deadline from this stream, cancellation from either. other is driven through iter.Pull, so it
// runs one coroutine that stops with the pipeline; other may itself be any pipeline, parallel
// stages included. Pairing is a pure function; for I/O on the pair use ZipCtx or a following
// MapCtx. A third source is another Zip whose fn extends the struct built by the first:
//
//	ids.Zip(users, func(id ID, u User) Row { return Row{ID: id, User: u} }).
//	    Zip(orders, func(r Row, o Order) Row { r.Order = o; return r })
func (s Stream[T]) Zip[O, R any](other Stream[O], fn func(a T, b O) R) Stream[R] {
	return s.ZipCtx(other, func(_ context.Context, a T, b O) (R, error) {
		return fn(a, b), nil
	})
}

// ZipCtx is Zip with a context-aware fn that may fail; a returned error takes the pair's
// position in the stream and, unless suppressed, ends it at the terminal. The context fn
// receives is the merged one described on Zip.
func (s Stream[T]) ZipCtx[O, R any](other Stream[O], fn func(ctx context.Context, a T, b O) (R, error)) Stream[R] {
	ctx := mergeCtx(s.ctx, other.ctx)
	return Stream[R]{options: s.options, ctx: ctx, seq: func(yield func(result[R]) bool) {
		next, stop := iter.Pull(other.seq)
		defer stop()
		var inCallback bool
		defer guardPanics(&inCallback, func(err error) { yield(result[R]{err: err}) })
		for item := range s.seq {
			if item.err != nil {
				if !yield(result[R]{err: item.err}) {
					return
				}
				continue
			}
			// The next value from other, forwarding its errors on the way.
			var partner result[O]
			for {
				o, ok := next()
				if !ok {
					return // other is exhausted: the pipeline ends, item is dropped
				}
				if o.err == nil {
					partner = o
					break
				}
				if !yield(result[R]{err: o.err}) {
					return
				}
			}
			inCallback = true
			val, err := fn(ctx, item.value, partner.value)
			inCallback = false
			if !yield(result[R]{value: val, err: err}) {
				return
			}
		}
	}}
}

// Through pipes the current stream into an external transformation function.
// It acts as a structural bridge to maintain fluent API chaining for operations that
// must be implemented as top-level functions (like Flatten) and for error policies
// (like policy.CircuitBreaker).
func (s Stream[T]) Through[R any](transform func(Stream[T]) Stream[R]) Stream[R] {
	return transform(s)
}

// Transform is the extension seam: it hands the raw element sequence to transform as a
// native iter.Seq2[T, error] and wraps what comes back into a Stream that keeps this
// stream's context and options. Unlike Seq, the raw sequence does not stop after a fatal
// error; every element, values and errors alike, reaches transform, which may forward,
// replace, drop or reorder them and may end the sequence early by returning. Together
// with Suppress this is all an error policy needs; policy.CircuitBreaker is written on it
// and nothing else.
//
// transform runs once, when the stream is built, and must only assemble the returned
// iterator; state that has to start fresh on every iteration belongs inside that iterator.
// Code in the returned iterator is not a callback: a panic there is not turned into
// ErrPanic, it propagates. A nil transform yields a single error and ends.
func (s Stream[T]) Transform[R any](transform func(iter.Seq2[T, error]) iter.Seq2[R, error]) Stream[R] {
	if transform == nil {
		return s.errStream[R](errors.New("chunkflow: Transform: nil transform"))
	}
	raw := func(yield func(T, error) bool) {
		for item := range s.seq {
			if !yield(item.value, item.err) {
				return
			}
		}
	}
	out := transform(raw)
	return s.derive(func(yield func(result[R]) bool) {
		for v, err := range out {
			if !yield(result[R]{value: v, err: err}) {
				return
			}
		}
	})
}

// Seq returns the stream as a native (value, error) iterator. Tolerated elements are
// yielded with an error matching ErrSuppressed; iteration stops after the first error
// that is not suppressed. To keep going past one, see Transform.
func (s Stream[T]) Seq() iter.Seq2[T, error] {
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
func (s Stream[T]) Collect() ([]T, error) {
	var res []T
	err := s.ForEach(func(item T) {
		res = append(res, item)
	})
	return res, err
}

// ForEach executes a side effect for each element and returns the first error.
func (s Stream[T]) ForEach(fn func(T)) error {
	return s.ForEachCtx(func(_ context.Context, item T) error {
		fn(item)
		return nil
	})
}

// ForEachCtx executes a context-aware side effect for each element and returns the first error.
func (s Stream[T]) ForEachCtx(fn func(ctx context.Context, item T) error) error {
	return s.each(func(item T) (bool, error) {
		return true, fn(s.ctx, item)
	})
}

// Count consumes the entire stream and returns the number of elements seen before the first error.
func (s Stream[T]) Count() (int, error) {
	n := 0
	err := s.each(func(T) (bool, error) {
		n++
		return true, nil
	})
	return n, err
}

// Drain exhausts the stream, discarding values, and returns the first error. It is the
// terminal for pipelines whose work happens in TapCtx or MapCtx side effects.
func (s Stream[T]) Drain() error {
	return s.each(func(T) (bool, error) { return true, nil })
}

// Reduce folds the stream into a single value: starting from init, fn is called as
// fn(acc, item) for every value and its result becomes the next accumulator. The
// accumulator type R is independent of the element type. On error the accumulator
// built so far is returned together with the error.
func (s Stream[T]) Reduce[R any](init R, fn func(acc R, item T) R) (R, error) {
	return s.ReduceCtx(init, func(_ context.Context, acc R, item T) (R, error) {
		return fn(acc, item), nil
	})
}

// ReduceCtx is Reduce with a context-aware fn that may fail; a returned error stops the
// fold and is returned with the accumulator built so far. A fold is sequential by nature,
// so unlike MapCtx it takes no StepOption.
func (s Stream[T]) ReduceCtx[R any](init R, fn func(ctx context.Context, acc R, item T) (R, error)) (R, error) {
	acc := init
	err := s.each(func(item T) (bool, error) {
		var err error
		acc, err = fn(s.ctx, acc, item)
		return true, err
	})
	return acc, err
}

// ReduceBy folds the stream into one accumulator per key: key picks the group of each
// value, and fn(acc, item) runs the fold of that group exactly as Reduce would, starting
// from init. It is the map-reduce terminal: grouping (init nil, fn appends), counting
// (init 0, fn adds one), sums, maxima and the like are all one call. Groups are kept in
// source order within each key. Values are materialised like Collect, so the result is
// bounded by the number of keys and the size of their accumulators, not by the stream.
//
// init is copied into every group by value. Start from a scalar, nil or a zero value
// and let fn allocate; a non-nil map, slice or pointer given as init would be shared
// between the groups. On error the map built so far is returned together with the error.
func (s Stream[T]) ReduceBy[K comparable, R any](key func(T) K, init R, fn func(acc R, item T) R) (map[K]R, error) {
	return s.ReduceByCtx(key, init, func(_ context.Context, acc R, item T) (R, error) {
		return fn(acc, item), nil
	})
}

// ReduceByCtx is ReduceBy with a context-aware fn that may fail; a returned error stops
// the fold and is returned with the map built so far. key stays a plain function: a key
// is a property of the value, work that needs the context belongs in a preceding MapCtx.
// Like ReduceCtx it is sequential and takes no StepOption.
func (s Stream[T]) ReduceByCtx[K comparable, R any](key func(T) K, init R, fn func(ctx context.Context, acc R, item T) (R, error)) (map[K]R, error) {
	groups := make(map[K]R)
	err := s.each(func(item T) (bool, error) {
		k := key(item)
		acc, ok := groups[k]
		if !ok {
			acc = init
		}
		acc, err := fn(s.ctx, acc, item)
		groups[k] = acc
		return true, err
	})
	return groups, err
}

// All verifies whether all elements satisfy the predicate. Short-circuits on the first mismatch.
//
// On an empty stream, or one consisting only of suppressed errors, All returns true
// (vacuous truth): no element could violate the predicate. All(p) == !Any(!p) always holds.
func (s Stream[T]) All(predicate func(T) bool) (bool, error) {
	return s.AllCtx(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// AllCtx verifies whether all elements satisfy the context-aware predicate.
func (s Stream[T]) AllCtx(predicate func(ctx context.Context, item T) (bool, error)) (bool, error) {
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
func (s Stream[T]) Any(predicate func(T) bool) (bool, error) {
	return s.AnyCtx(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// AnyCtx verifies whether at least one element satisfies the context-aware predicate.
func (s Stream[T]) AnyCtx(predicate func(ctx context.Context, item T) (bool, error)) (bool, error) {
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

// First returns the first value and stops pulling from the source. It returns ErrEmpty
// when the stream ends without a value, and the fatal error if one comes first.
func (s Stream[T]) First() (T, error) {
	var first T
	ok := false
	err := s.each(func(item T) (bool, error) {
		first, ok = item, true
		return false, nil
	})
	switch {
	case err != nil:
		var zero T
		return zero, err
	case !ok:
		return first, ErrEmpty
	}
	return first, nil
}

// Last consumes the entire stream and returns its final value. It returns ErrEmpty
// when the stream ends without a value, and the fatal error if one occurs.
func (s Stream[T]) Last() (T, error) {
	var last T
	ok := false
	err := s.each(func(item T) (bool, error) {
		last, ok = item, true
		return true, nil
	})
	switch {
	case err != nil:
		var zero T
		return zero, err
	case !ok:
		return last, ErrEmpty
	}
	return last, nil
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

// Compact drops consecutive duplicate values using ==. It requires comparable T, which a
// method cannot demand, so it is a top-level function; use it with Through. See
// Stream.CompactFunc for the semantics and the sorted-or-grouped input precondition.
func Compact[T comparable](stream Stream[T]) Stream[T] {
	return stream.CompactFunc(func(a, b T) bool { return a == b })
}

// Concat emits every element of the first stream, then of the second, and so on.
// Order is deterministic and a stream is not touched until all previous ones are
// exhausted. Errors pass through at their position. The result inherits the options of
// the first stream and a context that is cancelled when any source context is (see
// mergeContexts); with no arguments it returns an empty stream.
func Concat[T any](streams ...Stream[T]) Stream[T] {
	if len(streams) == 0 {
		return emptyStream[T]()
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

// Merge consumes all streams concurrently, one goroutine per source, and emits
// elements as they arrive. The interleaving is not deterministic. Errors pass through
// at their arrival position. When the consumer stops, or when the context of any
// source is cancelled, every source goroutine is released and the cancellation is
// reported as an error element carrying the original cause. The result inherits the
// options of the first stream and the merged context; with no arguments it returns an
// empty stream.
func Merge[T any](streams ...Stream[T]) Stream[T] {
	if len(streams) == 0 {
		return emptyStream[T]()
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

// Flatten unwraps a stream of slices into a flat stream of individual elements. It
// changes the element type from []E to E, which a method cannot express, so it is a
// top-level function; use it with Through.
func Flatten[E any](stream Stream[[]E]) Stream[E] {
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
	// unordered lets a parallel step emit results as they arrive instead of in source order.
	unordered bool
	// readAhead bounds the reorder window of an ordered parallel step to readAhead*concurrency
	// items pulled from the source and not yet emitted. Not user-settable yet; the default
	// keeps every worker busy while the head-of-line item is only as slow as its neighbours.
	readAhead int
	onError   func(error)
	err       error
}

func defaultOptions() options {
	return options{
		concurrency: 1,
		readAhead:   2,
		onError:     func(error) {},
	}
}

// window is the number of items an ordered parallel step may hold between the source
// and its output.
func (o options) window() int {
	return o.readAhead * o.concurrency
}

// mergeContexts is mergeCtx over the contexts of streams, in order.
func mergeContexts[T any](streams []Stream[T]) context.Context {
	ctxs := make([]context.Context, len(streams))
	for i, s := range streams {
		ctxs[i] = s.ctx
	}
	return mergeCtx(ctxs...)
}

// mergeCtx returns a context that carries the values and deadline of the first context and
// is cancelled as soon as any of them is cancelled, with the original cause preserved (see
// context.Cause). Identical contexts are registered once. Contexts are compared with ==,
// which holds for every context.Context produced by the standard library.
func mergeCtx(ctxs ...context.Context) context.Context {
	head := ctxs[0]
	seen := []context.Context{head}
	for _, c := range ctxs[1:] {
		if !slices.Contains(seen, c) {
			seen = append(seen, c)
		}
	}
	others := seen[1:]
	if len(others) == 0 {
		return head // everything shares the head context; nothing to merge
	}

	merged, cancel := context.WithCancelCause(head)
	link := func(other context.Context) {
		context.AfterFunc(other, func() { cancel(context.Cause(other)) })
	}
	link(others[0]) // vet's lostcancel wants a guaranteed use; others is never empty here
	for _, other := range others[1:] {
		link(other)
	}
	return merged
}

// emptyStream returns a stream that ends immediately, for combinators called with no inputs.
func emptyStream[T any]() Stream[T] {
	return Stream[T]{
		options: defaultOptions(),
		ctx:     context.Background(),
		seq:     func(func(result[T]) bool) {},
	}
}

func (s Stream[T]) getOptions(opts ...StepOption) options {
	o := s.options
	for _, apply := range opts {
		apply(&o)
	}
	return o
}

// derive builds a new stream of another element type that inherits ctx and options.
func (s Stream[T]) derive[R any](seq iter.Seq[result[R]]) Stream[R] {
	return Stream[R]{options: s.options, ctx: s.ctx, seq: seq}
}

// errStream builds a stream that emits a single error and ends.
func (s Stream[T]) errStream[R any](err error) Stream[R] {
	return s.derive(func(yield func(result[R]) bool) {
		yield(result[R]{err: err})
	})
}

// each drives every terminal operation: it reports every error to the OnError hook,
// skips elements carrying a suppressed error, returns the first other error, and stops
// early when fn returns false.
func (s Stream[T]) each(fn func(T) (bool, error)) (err error) {
	var inCallback bool
	defer guardPanics(&inCallback, func(perr error) {
		s.options.onError(perr)
		err = perr
	})
	for item := range s.seq {
		if item.err != nil {
			s.options.onError(item.err)
			if isSuppressed(item.err) {
				continue
			}
			return item.err
		}
		inCallback = true
		next, err := fn(item.value)
		inCallback = false
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

// stream assembles a Stream from the builder's context with default options.
func (b Builder) stream[T any](seq iter.Seq[result[T]]) Stream[T] {
	return Stream[T]{options: defaultOptions(), ctx: b.ctx, seq: seq}
}

// mapCtxConcurrent is the WithParallel(n > 1) path of MapCtx and FilterCtx, returned as
// a bare sequence so callers can wrap it in any element type. Ordered by default;
// WithUnordered selects the cheaper pool that emits results as they arrive.
func (s Stream[T]) mapCtxConcurrent[R any](mapFn func(ctx context.Context, item T) (R, error), o options) iter.Seq[result[R]] {
	if o.unordered {
		return s.mapCtxUnordered(mapFn, o.concurrency)
	}
	return s.mapCtxOrdered(mapFn, o.concurrency, o.window())
}

// ticket is the slot reserved for one source element in an ordered pool. The channel
// has room for exactly one result, so whoever fills it never blocks.
type ticket[T, R any] struct {
	value T
	done  chan result[R]
}

// mapCtxOrdered runs mapFn on n workers and emits results in source order. The feeder
// numbers nothing: it creates one ticket per element, pushes the ticket into the ordered
// queue and, for values, also hands it to a worker through inChan; upstream errors get a
// pre-filled ticket and never see a worker. The emitter walks the queue in order and blocks
// on each ticket, so results are reordered without a heap. The window semaphore bounds the
// items pulled from the source and not yet emitted to k: the feeder takes a permit before
// every pull, the emitter returns one after every yield. A consumer stopping early cancels
// ctx, which releases the feeder and the workers; a worker that is mid-callback finishes
// it and drops the result into a buffered channel, so nothing blocks on the way out.
func (s Stream[T]) mapCtxOrdered[R any](mapFn func(ctx context.Context, item T) (R, error), concurrency, k int) iter.Seq[result[R]] {
	return func(yield func(result[R]) bool) {
		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()

		// At most k items are in flight (the window permits), and every one of them occupies
		// at most one slot in queue and one in inChan, so with capacity k neither send below
		// can block; the window is the only place where the feeder waits.
		queue := make(chan ticket[T, R], k)
		inChan := make(chan ticket[T, R], k)
		window := make(chan struct{}, k)

		// feeder
		go func() {
			defer close(queue)
			defer close(inChan)
			acquire := func() bool {
				select {
				case window <- struct{}{}:
					return true
				case <-ctx.Done():
					return false
				}
			}
			if !acquire() {
				return
			}
			for item := range s.seq {
				t := ticket[T, R]{value: item.value, done: make(chan result[R], 1)}
				queue <- t
				if item.err != nil {
					t.done <- result[R]{err: item.err}
				} else {
					inChan <- t
				}
				if !acquire() { // permit for the next pull
					return
				}
			}
		}()

		// workers
		for range concurrency {
			go func() {
				var inCallback bool
				var cur ticket[T, R]
				// A panicking worker reports the panic in the ticket it was working on and
				// exits; the consumer treats ErrPanic as fatal and cancels the rest of the pool.
				defer guardPanics(&inCallback, func(err error) { cur.done <- result[R]{err: err} })
				for {
					select {
					case <-ctx.Done():
						return
					case t, ok := <-inChan:
						if !ok {
							return
						}
						cur = t
						inCallback = true
						res, err := mapFn(ctx, t.value)
						inCallback = false
						t.done <- result[R]{value: res, err: err}
					}
				}
			}()
		}

		// emitter
		for t := range queue {
			if !yield(<-t.done) {
				return
			}
			<-window
		}
		// The feeder bails out silently on ctx.Done(); make sure a cancellation of the
		// stream context still surfaces to the consumer as an error.
		if err := s.ctx.Err(); err != nil {
			yield(result[R]{err: err})
		}
	}
}

// mapCtxUnordered is the WithUnordered pool. It wires three kinds of goroutines: a feeder
// that reads the upstream sequence into inChan (forwarding upstream errors straight to
// outChan), n workers that apply mapFn and push results to outChan, and a closer that
// closes outChan once every writer has exited. Every writer is tracked by the WaitGroup
// so the channel is never closed under a live sender. A consumer stopping early cancels
// ctx, which releases all of them.
func (s Stream[T]) mapCtxUnordered[R any](mapFn func(ctx context.Context, item T) (R, error), concurrency int) iter.Seq[result[R]] {
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
				var inCallback bool
				// A panicking worker reports the panic as its last result and exits; the
				// consumer treats ErrPanic as fatal and cancels the rest of the pool.
				defer guardPanics(&inCallback, func(err error) {
					select {
					case outChan <- result[R]{err: err}:
					case <-ctx.Done():
					}
				})
				for {
					select {
					case <-ctx.Done():
						return
					case val, ok := <-inChan:
						if !ok {
							return
						}
						inCallback = true
						res, err := mapFn(ctx, val)
						inCallback = false
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
