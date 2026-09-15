package chunkflow

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"sync"
)

// IoStream represents a lazily evaluated, context-aware pipeline whose elements may
// carry errors. It mirrors the Stream API; every method that accepts a user callback
// has an *Async counterpart that receives a context and may return an error.
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

// WithLogger sets the logger used for diagnostics.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

func defaultOptions() options {
	return options{
		concurrency: 1,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
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
func derive[T, R any](s IoStream[T], seq iter.Seq[result[R]]) IoStream[R] {
	return IoStream[R]{options: s.options, ctx: s.ctx, seq: seq}
}

func errStream[T, R any](s IoStream[T], err error) IoStream[R] {
	return derive[T, R](s, func(yield func(result[R]) bool) {
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
		return errStream[T, R](s, fmt.Errorf("concurrency must be at least 1"))
	}

	if o.concurrency == 1 {
		return derive[T, R](s, func(yield func(result[R]) bool) {
			for item := range s.seq {
				if item.err != nil {
					yield(result[R]{err: item.err})
					return
				}
				val, err := mapFn(s.ctx, item.value)
				if err != nil {
					yield(result[R]{err: err})
					return
				}
				if !yield(result[R]{value: val}) {
					return
				}
			}
		})
	}

	return derive[T, R](s, func(yield func(result[R]) bool) {
		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()

		outChan := make(chan result[R], o.concurrency)
		runConcurrent(ctx, s.seq, o.concurrency, mapFn, outChan)

		for res := range outChan {
			if res.err != nil {
				yield(res)
				return
			}
			if !yield(res) {
				return
			}
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
		return errStream[T, T](s, fmt.Errorf("concurrency must be at least 1"))
	}

	if o.concurrency == 1 {
		return derive[T, T](s, func(yield func(result[T]) bool) {
			for item := range s.seq {
				if item.err != nil {
					yield(item)
					return
				}
				match, err := predicate(s.ctx, item.value)
				if err != nil {
					yield(result[T]{err: err})
					return
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

	return derive[T, T](s, func(yield func(result[T]) bool) {
		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()

		outChan := make(chan result[filtered], o.concurrency)
		runConcurrent(ctx, s.seq, o.concurrency, func(ctx context.Context, item T) (filtered, error) {
			match, err := predicate(ctx, item)
			return filtered{val: item, match: match}, err
		}, outChan)

		for res := range outChan {
			if res.err != nil {
				yield(result[T]{err: res.err})
				return
			}
			if res.value.match && !yield(result[T]{value: res.value.val}) {
				return
			}
		}
	})
}

// Take consumes at most nr elements from the stream and then short-circuits.
func (s IoStream[T]) Take(nr int) IoStream[T] {
	return derive[T, T](s, func(yield func(result[T]) bool) {
		if nr <= 0 {
			return
		}
		count := 0
		for item := range s.seq {
			if item.err != nil {
				yield(item)
				return
			}
			if !yield(item) {
				return
			}
			count++
			if count == nr {
				return
			}
		}
	})
}

// Skip bypasses the first nr elements and emits the remainder of the stream.
func (s IoStream[T]) Skip(nr int) IoStream[T] {
	return derive[T, T](s, func(yield func(result[T]) bool) {
		skipped := 0
		for item := range s.seq {
			if item.err != nil {
				yield(item)
				return
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
func (s IoStream[T]) Chunk[R []T](size int) IoStream[R] {
	if size < 1 {
		return errStream[T, R](s, fmt.Errorf("chunk size must be >= 1"))
	}

	return derive[T, R](s, func(yield func(result[R]) bool) {
		chunk := make([]T, 0, size)
		for item := range s.seq {
			if item.err != nil {
				yield(result[R]{err: item.err})
				return
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

// CircuitBreaker interrupts the stream with a critical error if maxConsecutiveFailures
// errors occur in a row. Errors below the threshold are suppressed.
func (s IoStream[T]) CircuitBreaker(maxConsecutiveFailures int) IoStream[T] {
	if maxConsecutiveFailures < 1 {
		return errStream[T, T](s, fmt.Errorf("threshold must be >= 1"))
	}

	return derive[T, T](s, func(yield func(result[T]) bool) {
		consecutiveFailures := 0
		for item := range s.seq {
			if item.err != nil {
				consecutiveFailures++
				if consecutiveFailures >= maxConsecutiveFailures {
					yield(result[T]{
						err: fmt.Errorf("circuit breaker tripped after %d consecutive errors: %w",
							consecutiveFailures, item.err),
					})
					return
				}
				continue
			}
			consecutiveFailures = 0
			if !yield(item) {
				return
			}
		}
	})
}

// IoFlatten unwraps a stream of slices into a flat stream of individual elements.
// It is the IoStream counterpart of Flatten; use it with Through.
func IoFlatten[E any](stream IoStream[[]E]) IoStream[E] {
	return derive[[]E, E](stream, func(yield func(result[E]) bool) {
		for chunk := range stream.seq {
			if chunk.err != nil {
				yield(result[E]{err: chunk.err})
				return
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

// Seq returns the stream as a native (value, error) iterator. Iteration stops
// after the first error is yielded.
func (s IoStream[T]) Seq() iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for item := range s.seq {
			if !yield(item.value, item.err) || item.err != nil {
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
	return s.ForEachAsync(func(_ context.Context, item T) error {
		fn(item)
		return nil
	})
}

// ForEachAsync executes a context-aware side effect for each element and returns the first error.
func (s IoStream[T]) ForEachAsync(fn func(ctx context.Context, item T) error) error {
	for item := range s.seq {
		if item.err != nil {
			return item.err
		}
		if err := fn(s.ctx, item.value); err != nil {
			return err
		}
	}
	return nil
}

// Count consumes the entire stream and returns the number of elements seen before the first error.
func (s IoStream[T]) Count() (int, error) {
	n := 0
	for item := range s.seq {
		if item.err != nil {
			return n, item.err
		}
		n++
	}
	return n, nil
}

// Exec exhausts the stream, discarding values, and returns the first error.
func (s IoStream[T]) Exec() error {
	for item := range s.seq {
		if item.err != nil {
			return item.err
		}
	}
	return nil
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
	for item := range s.seq {
		if item.err != nil {
			return acc, item.err
		}
		var err error
		if acc, err = fn(s.ctx, item.value, acc); err != nil {
			return acc, err
		}
	}
	return acc, nil
}

// All verifies whether all elements satisfy the predicate. Short-circuits on the first mismatch.
func (s IoStream[T]) All(predicate func(T) bool) (bool, error) {
	return s.AllAsync(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// AllAsync verifies whether all elements satisfy the context-aware predicate.
func (s IoStream[T]) AllAsync(predicate func(ctx context.Context, item T) (bool, error)) (bool, error) {
	for item := range s.seq {
		if item.err != nil {
			return false, item.err
		}
		match, err := predicate(s.ctx, item.value)
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}

// Any verifies whether at least one element satisfies the predicate. Short-circuits on the first match.
func (s IoStream[T]) Any(predicate func(T) bool) (bool, error) {
	return s.AnyAsync(func(_ context.Context, item T) (bool, error) {
		return predicate(item), nil
	})
}

// AnyAsync verifies whether at least one element satisfies the context-aware predicate.
func (s IoStream[T]) AnyAsync(predicate func(ctx context.Context, item T) (bool, error)) (bool, error) {
	for item := range s.seq {
		if item.err != nil {
			return false, item.err
		}
		match, err := predicate(s.ctx, item.value)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

// First consumes at most one element. Returns (zero, false, nil) for an empty stream.
func (s IoStream[T]) First() (T, bool, error) {
	for item := range s.seq {
		if item.err != nil {
			var zero T
			return zero, false, item.err
		}
		return item.value, true, nil
	}
	var zero T
	return zero, false, nil
}

// Last consumes the entire stream and returns the final element.
func (s IoStream[T]) Last() (T, bool, error) {
	var last T
	var ok bool
	for item := range s.seq {
		if item.err != nil {
			return last, ok, item.err
		}
		last, ok = item.value, true
	}
	return last, ok, nil
}

// ---------------------------------------------------------------------------
// Concurrency plumbing
// ---------------------------------------------------------------------------

// runConcurrent wires a feeder goroutine, a worker pool and a closer:
// the feeder reads inputSeq into inChan, workers apply workerFn and push to outputChan,
// and the closer closes outputChan once all workers exit.
func runConcurrent[T any, R any](
	ctx context.Context,
	inputSeq iter.Seq[result[T]],
	concurrency int,
	workerFn func(ctx context.Context, item T) (R, error),
	outputChan chan<- result[R],
) {
	inChan := make(chan T, concurrency)

	go func() {
		defer close(inChan)
		for item := range inputSeq {
			if item.err != nil {
				select {
				case outputChan <- result[R]{err: item.err}:
				case <-ctx.Done():
				}
				return
			}
			select {
			case inChan <- item.value:
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(concurrency)
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
