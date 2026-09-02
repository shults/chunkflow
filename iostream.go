package chunkflow

import (
	"context"
	"iter"
	"sync"
)

type IoStream[T any] struct {
	ctx context.Context
	seq iter.Seq[ioItem[T]]
}

type ioItem[T any] struct {
	value T
	err   error
}

func NewIoStream[T any](ctx context.Context, seq iter.Seq[T]) IoStream[T] {
	return IoStream[T]{
		ctx: ctx,
		seq: func(yield func(ioItem[T]) bool) {
			for item := range seq {
				if !yield(ioItem[T]{value: item, err: ctx.Err()}) {
					return
				}
			}
		},
	}
}

func (s IoStream[T]) TryMap[R any](mapFn func(ctx context.Context, item T) (R, error)) IoStream[R] {
	var zero R

	return IoStream[R]{
		ctx: s.ctx,
		seq: func(yield func(ioItem[R]) bool) {
			for item := range s.seq {
				if item.err != nil {
					yield(ioItem[R]{value: zero, err: item.err})
					return
				}

				if err := s.ctx.Err(); err != nil {
					yield(ioItem[R]{value: zero, err: err})
					return
				}

				res, err := mapFn(s.ctx, item.value)
				if err != nil {
					yield(ioItem[R]{value: zero, err: err})
					return
				}

				if !yield(ioItem[R]{value: res, err: nil}) {
					return
				}
			}
		},
	}
}

func (s IoStream[T]) Map[R any](mapFn func(T) R) IoStream[R] {
	var zero R

	return IoStream[R]{
		ctx: s.ctx,
		seq: func(yield func(ioItem[R]) bool) {
			for item := range s.seq {
				if item.err != nil {
					yield(ioItem[R]{value: zero, err: item.err})
					return
				}

				if err := s.ctx.Err(); err != nil {
					yield(ioItem[R]{value: zero, err: err})
					return
				}

				if !yield(ioItem[R]{value: mapFn(item.value), err: nil}) {
					return
				}
			}
		},
	}
}

func (s IoStream[T]) ParallelMap[R any](
	concurrency int,
	mapFn func(ctx context.Context, item T) (R, error),
) IoStream[R] {
	if concurrency < 1 {
		panic("concurrency must be at least 1")
	}

	return IoStream[R]{
		ctx: s.ctx,
		seq: func(yield func(ioItem[R]) bool) {
			ctx, cancel := context.WithCancel(s.ctx)
			defer cancel()

			inChan := make(chan T, concurrency)
			outChan := make(chan ioItem[R], concurrency)

			// 1. Feeder: reeds from input sequence and feeds input channel
			go func() {
				defer close(inChan)
				for item := range s.seq {
					if item.err != nil {
						select {
						case outChan <- ioItem[R]{err: item.err}:
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

			// 2. Worker Pool reads from input channel and pushes further
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

							res, err := mapFn(ctx, val)
							select {
							case outChan <- ioItem[R]{value: res, err: err}:
							case <-ctx.Done():
								return
							}
						}
					}
				}()
			}

			// closer: stop work when all workers end their job
			go func() {
				wg.Wait()
				close(outChan)
			}()

			// 4. consume and push further
			for res := range outChan {
				if res.err != nil {
					yield(res)
					return // stop loop, cancel ctx, cleanup goroutines
				}

				// consumer stopped stream
				if !yield(res) {
					return // stop loop, cancel ctx, cleanup goroutines
				}
			}
		},
	}
}

func (s IoStream[T]) Exec() error {
	for item := range s.seq {
		if item.err != nil {
			return item.err
		}
	}

	return nil
}
