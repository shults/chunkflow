package policy

import (
	"errors"
	"fmt"
	"iter"

	"github.com/shults/chunkflow"
)

// CircuitBreaker tolerates up to maxConsecutiveFailures-1 errors in a row and trips on
// the next one, interrupting the stream with a wrapped error. A successful element
// resets the counter.
//
// Tolerated errors are not dropped: they are re-emitted marked with chunkflow.ErrSuppressed,
// the original error still in the chain, so downstream terminal operations skip them
// while consumers of Seq can observe them. Errors already marked as suppressed, by an
// upstream breaker or by a callback via Suppress, pass through without affecting the
// counter. Context errors and recovered panics (chunkflow.ErrPanic) are never tolerated:
// they pass through untouched and end the stream.
//
// After a parallel stage "consecutive" follows source order, which is what the stage
// emits unless it runs WithUnordered; then it follows arrival order. A threshold below 1
// is invalid: the stream emits a single error and ends.
func CircuitBreaker[T any](maxConsecutiveFailures int) func(chunkflow.Stream[T]) chunkflow.Stream[T] {
	return func(s chunkflow.Stream[T]) chunkflow.Stream[T] {
		if maxConsecutiveFailures < 1 {
			return s.Transform(func(iter.Seq2[T, error]) iter.Seq2[T, error] {
				return func(yield func(T, error) bool) {
					var zero T
					yield(zero, fmt.Errorf("policy: CircuitBreaker(%d): threshold must be at least 1", maxConsecutiveFailures))
				}
			})
		}
		return s.Transform(func(in iter.Seq2[T, error]) iter.Seq2[T, error] {
			return func(yield func(T, error) bool) {
				var zero T
				failures := 0
				for v, err := range in {
					switch {
					case err == nil:
						failures = 0
					case errors.Is(err, chunkflow.ErrSuppressed):
						// already tolerated upstream; not ours to count
					default:
						failures++
						if failures >= maxConsecutiveFailures {
							yield(zero, fmt.Errorf("circuit breaker tripped after %d consecutive errors: %w", failures, err))
							return
						}
						marked := chunkflow.Suppress(fmt.Errorf("circuit breaker failure %d/%d: %w", failures, maxConsecutiveFailures, err))
						if !errors.Is(marked, chunkflow.ErrSuppressed) {
							// Suppress refused: a bug or a cancellation. Pass it through and end.
							yield(zero, err)
							return
						}
						v, err = zero, marked
					}
					if !yield(v, err) {
						return
					}
				}
			}
		})
	}
}
