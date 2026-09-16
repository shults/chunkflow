package chunkflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

// failing maps items 2, 3 and 4 to an error; everything else passes through.
func failing(_ context.Context, i int) (int, error) {
	if i >= 2 && i <= 4 {
		return 0, errBoom
	}
	return i, nil
}

func TestStream_CircuitBreaker(t *testing.T) {
	ctx := t.Context()

	t.Run("tolerates errors below the threshold and keeps processing the source", func(t *testing.T) {
		var mapped []int
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(func(ctx context.Context, i int) (int, error) {
				mapped = append(mapped, i)
				return failing(ctx, i)
			}).
			CircuitBreaker(5). // 3 consecutive failures < 5
			Collect()

		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, res, "items after the failures must still be delivered")
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, mapped, "the source must be consumed to the end")
	})

	t.Run("trips when consecutive failures reach the threshold", func(t *testing.T) {
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(failing).
			CircuitBreaker(3).
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
			CircuitBreaker(2).
			Collect()

		require.NoError(t, err)
		assert.Equal(t, []int{0, 2, 4, 6}, res)
	})

	t.Run("threshold below 1 is an error", func(t *testing.T) {
		_, err := chunkflow.New(ctx).Seq(seq.Items(1)).CircuitBreaker(0).Collect()
		require.ErrorContains(t, err, "threshold must be >= 1")
	})

	t.Run("suppressed errors are observable through Seq", func(t *testing.T) {
		var values []int
		var suppressed []error

		for v, err := range chunkflow.New(ctx).Seq(seq.Range(0, 10)).MapCtx(failing).CircuitBreaker(5).Seq() {
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
		assert.Equal(t, "suppressed by circuit breaker (failure 1/5): boom", suppressed[0].Error())
		assert.Equal(t, "suppressed by circuit breaker (failure 3/5): boom", suppressed[2].Error())
	})

	t.Run("Seq stops after a fatal error but not after a suppressed one", func(t *testing.T) {
		var errs []error
		for _, err := range chunkflow.New(ctx).Seq(seq.Range(0, 10)).MapCtx(failing).CircuitBreaker(3).Seq() {
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
			CircuitBreaker(100).
			CircuitBreaker(1).
			Collect()

		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, res)
	})

	t.Run("context errors are never suppressed", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()

		_, err := chunkflow.
			New(cctx).Seq(seq.Numbers(0)).
			CircuitBreaker(1000).
			Take(3).
			Collect()

		require.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, chunkflow.ErrSuppressed)
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
			CircuitBreaker(50).
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
		res, err := chunkflow.New(ctx).Seq2(src).CircuitBreaker(2).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 2, 4, 5}, res)
	})
}

// tolerant is 0..9 with 2,3,4 failing and a breaker that never trips, so the
// suppressed errors reach whatever comes next.
func tolerant(ctx context.Context) chunkflow.Stream[int] {
	return chunkflow.New(ctx).Seq(seq.Range(0, 10)).MapCtx(failing).CircuitBreaker(100)
}

func TestStream_TerminalsSkipSuppressedErrors(t *testing.T) {
	ctx := t.Context()

	t.Run("ForEach and Count", func(t *testing.T) {
		var seen []int
		require.NoError(t, tolerant(ctx).ForEach(func(i int) { seen = append(seen, i) }))
		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, seen)

		n, err := tolerant(ctx).Count()
		require.NoError(t, err)
		assert.Equal(t, 7, n)
	})

	t.Run("Exec and Reduce", func(t *testing.T) {
		require.NoError(t, tolerant(ctx).Exec())

		sum, err := tolerant(ctx).Reduce(0, func(acc, item int) int { return acc + item })
		require.NoError(t, err)
		assert.Equal(t, 0+1+5+6+7+8+9, sum)
	})

	t.Run("All and Any", func(t *testing.T) {
		all, err := tolerant(ctx).All(func(i int) bool { return i < 2 || i > 4 })
		require.NoError(t, err)
		assert.True(t, all, "the failed items must not be evaluated")

		any, err := tolerant(ctx).Any(func(i int) bool { return i == 3 })
		require.NoError(t, err)
		assert.False(t, any, "3 failed upstream, so it must never be seen")

		any, err = tolerant(ctx).Any(func(i int) bool { return i == 5 })
		require.NoError(t, err)
		assert.True(t, any)
	})

	t.Run("First and Last", func(t *testing.T) {
		// make the very first items fail so First has to skip suppressed errors
		s := chunkflow.New(ctx).Seq(seq.Range(2, 10)).MapCtx(failing).CircuitBreaker(100)

		first, ok, err := s.First()
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 5, first)

		last, ok, err := tolerant(ctx).Last()
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 9, last)
	})

	t.Run("callback errors are still fatal", func(t *testing.T) {
		errCb := errors.New("callback")
		err := tolerant(ctx).ForEachCtx(func(_ context.Context, i int) error {
			if i == 6 {
				return errCb
			}
			return nil
		})
		require.ErrorIs(t, err, errCb)
	})
}

func TestStream_ErrorsFlowThroughIntermediateStages(t *testing.T) {
	ctx := t.Context()

	source := func() chunkflow.Stream[int] {
		return chunkflow.New(ctx).Seq(seq.Range(0, 10)).MapCtx(failing)
	}
	swallow := func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
		return s.CircuitBreaker(100)
	}

	t.Run("Take does not count errors", func(t *testing.T) {
		res, err := source().Take(4).Through(swallow).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5, 6}, res)
	})

	t.Run("Skip does not count errors", func(t *testing.T) {
		res, err := source().Skip(3).Through(swallow).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{6, 7, 8, 9}, res)
	})

	t.Run("Filter and Map pass errors through", func(t *testing.T) {
		res, err := source().
			Filter(func(i int) bool { return i%2 == 1 }).
			Map(func(i int) int { return i * 10 }).
			Through(swallow).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{10, 50, 70, 90}, res)
	})

	t.Run("Chunk keeps the partial chunk across an error", func(t *testing.T) {
		res, err := source().Chunk[[]int](3).CircuitBreaker(100).Collect()
		require.NoError(t, err)
		assert.Equal(t, [][]int{{0, 1, 5}, {6, 7, 8}, {9}}, res)
	})

	t.Run("Flatten passes errors through", func(t *testing.T) {
		res, err := source().Chunk[[]int](2).Through(chunkflow.Flatten).Through(swallow).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, res)
	})

	t.Run("suppressed errors survive a round trip through Seq and New().Seq2", func(t *testing.T) {
		res, err := chunkflow.New(ctx).Seq2(tolerant(ctx).Seq()).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, res)
	})

	t.Run("without a breaker terminals still fail fast", func(t *testing.T) {
		var mapped int
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(func(ctx context.Context, i int) (int, error) {
				mapped++
				return failing(ctx, i)
			}).
			Collect()
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, []int{0, 1}, res)
		assert.Equal(t, 3, mapped, "source consumption must stop right after the first error")
	})
}
