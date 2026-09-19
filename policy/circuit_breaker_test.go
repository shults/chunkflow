package policy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/policy"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var errBoom = errors.New("boom")

// failing maps items 2, 3 and 4 to an error; everything else passes through.
func failing(_ context.Context, i int) (int, error) {
	if i >= 2 && i <= 4 {
		return 0, errBoom
	}
	return i, nil
}

func TestCircuitBreaker(t *testing.T) {
	ctx := t.Context()

	t.Run("tolerates errors below the threshold and keeps processing the source", func(t *testing.T) {
		var mapped []int
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(func(ctx context.Context, i int) (int, error) {
				mapped = append(mapped, i)
				return failing(ctx, i)
			}).
			Through(policy.CircuitBreaker[int](5)). // 3 consecutive failures < 5
			Collect()

		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, res, "items after the failures must still be delivered")
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, mapped, "the source must be consumed to the end")
	})

	t.Run("trips when consecutive failures reach the threshold", func(t *testing.T) {
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(failing).
			Through(policy.CircuitBreaker[int](3)).
			Collect()

		require.ErrorIs(t, err, errBoom)
		require.ErrorContains(t, err, "circuit breaker tripped after 3 consecutive errors")
		require.NotErrorIs(t, err, chunkflow.ErrSuppressed, "a tripped error is fatal, not suppressed")
		assert.Equal(t, []int{0, 1}, res)
	})

	t.Run("a success resets the failure counter", func(t *testing.T) {
		// errors on odd items: never two in a row, so threshold 2 must never trip
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, 8)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i%2 == 1 {
					return 0, errBoom
				}
				return i, nil
			}).
			Through(policy.CircuitBreaker[int](2)).
			Collect()

		require.NoError(t, err)
		assert.Equal(t, []int{0, 2, 4, 6}, res)
	})

	t.Run("threshold below 1 is an error", func(t *testing.T) {
		_, err := chunkflow.New(ctx).Seq(seq.Items(1)).Through(policy.CircuitBreaker[int](0)).Collect()
		require.ErrorContains(t, err, "threshold must be at least 1")
	})

	t.Run("suppressed errors are observable through Seq", func(t *testing.T) {
		var values []int
		var suppressed []error

		for v, err := range chunkflow.New(ctx).Seq(seq.Range(0, 10)).MapCtx(failing).Through(policy.CircuitBreaker[int](5)).Seq() {
			if err == nil {
				values = append(values, v)
				continue
			}
			require.ErrorIs(t, err, chunkflow.ErrSuppressed)
			require.ErrorIs(t, err, errBoom, "the original error must stay in the chain")
			suppressed = append(suppressed, err)
		}

		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, values)
		require.Len(t, suppressed, 3)
		assert.Equal(t, "suppressed: circuit breaker failure 1/5: boom", suppressed[0].Error())
		assert.Equal(t, "suppressed: circuit breaker failure 3/5: boom", suppressed[2].Error())
	})

	t.Run("Seq stops after a fatal error but not after a suppressed one", func(t *testing.T) {
		var errs []error
		for _, err := range chunkflow.New(ctx).Seq(seq.Range(0, 10)).MapCtx(failing).Through(policy.CircuitBreaker[int](3)).Seq() {
			if err != nil {
				errs = append(errs, err)
			}
		}
		require.Len(t, errs, 3)
		require.ErrorIs(t, errs[0], chunkflow.ErrSuppressed)
		require.ErrorIs(t, errs[1], chunkflow.ErrSuppressed)
		require.NotErrorIs(t, errs[2], chunkflow.ErrSuppressed)
		assert.ErrorContains(t, errs[2], "circuit breaker tripped")
	})

	t.Run("chained breakers do not count each other's suppressed errors", func(t *testing.T) {
		// inner breaker tolerates everything; outer one with threshold 1 would trip on
		// any live error, so it must see only suppressed ones.
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(failing).
			Through(policy.CircuitBreaker[int](100)).
			Through(policy.CircuitBreaker[int](1)).
			Collect()

		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, res)
	})

	t.Run("a panic upstream passes through untouched and ends the stream", func(t *testing.T) {
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i == 2 {
					panic("bug")
				}
				return i, nil
			}).
			Through(policy.CircuitBreaker[int](1000)).
			Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
		require.NotErrorIs(t, err, chunkflow.ErrSuppressed)
		assert.Equal(t, []int{0, 1}, res)
	})

	t.Run("the stream can be iterated twice, the counter starts fresh", func(t *testing.T) {
		s := chunkflow.New(ctx).Seq(seq.Range(0, 10)).MapCtx(failing).Through(policy.CircuitBreaker[int](4))
		for range 2 {
			res, err := s.Collect()
			require.NoError(t, err)
			assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, res)
		}
	})

	t.Run("stops pulling when the consumer is done", func(t *testing.T) {
		pulled := 0
		res, err := chunkflow.
			New(ctx).Seq(seq.Numbers(0)).
			Tap(func(int) { pulled++ }).
			MapCtx(failing).
			Through(policy.CircuitBreaker[int](100)).
			Take(3). // 0, 1, then 2-4 suppressed, then 5
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5}, res)
		assert.Equal(t, 6, pulled)
	})

	t.Run("context errors are never suppressed", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()

		_, err := chunkflow.
			New(cctx).Seq(seq.Numbers(0)).
			Through(policy.CircuitBreaker[int](1000)).
			Take(3).
			Collect()

		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, chunkflow.ErrSuppressed)
	})

	t.Run("works after a parallel stage", func(t *testing.T) {
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, 100)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i%10 == 0 {
					return 0, errBoom
				}
				return i, nil
			}, chunkflow.WithParallel(4)).
			Through(policy.CircuitBreaker[int](50)).
			Collect()

		require.NoError(t, err)
		assert.Len(t, res, 90)
	})

	t.Run("directly after an erroring Seq2 source", func(t *testing.T) {
		src := func(yield func(int, error) bool) {
			for i := range 6 {
				var e error
				if i == 1 || i == 3 {
					e = errBoom
				}
				if !yield(i, e) {
					return
				}
			}
		}
		res, err := chunkflow.New(ctx).Seq2(src).Through(policy.CircuitBreaker[int](2)).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 2, 4, 5}, res)
	})
}
