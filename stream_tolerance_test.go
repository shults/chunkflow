package chunkflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/policy"
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

// tolerant is 0..9 with 2,3,4 failing and a breaker that never trips, so the
// suppressed errors reach whatever comes next.
func tolerant(ctx context.Context) chunkflow.Stream[int] {
	return chunkflow.New(ctx).Seq(seq.Range(0, 10)).MapCtx(failing).Through(policy.CircuitBreaker[int](100))
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
		require.NoError(t, tolerant(ctx).Drain())

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
		s := chunkflow.New(ctx).Seq(seq.Range(2, 10)).MapCtx(failing).Through(policy.CircuitBreaker[int](100))

		first, err := s.First()
		require.NoError(t, err)
		assert.Equal(t, 5, first)

		last, err := tolerant(ctx).Last()
		require.NoError(t, err)
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
		return s.Through(policy.CircuitBreaker[int](100))
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
		res, err := source().Chunk[[]int](3).Through(policy.CircuitBreaker[[]int](100)).Collect()
		require.NoError(t, err)
		assert.Equal(t, [][]int{{0, 1, 5}, {6, 7, 8}, {9}}, res)
	})

	t.Run("Flatten passes errors through", func(t *testing.T) {
		res, err := source().Chunk[[]int](2).Through(chunkflow.Flatten).Through(swallow).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, res)
	})

	t.Run("suppressed errors survive a round trip through Seq and New().SeqErr", func(t *testing.T) {
		res, err := chunkflow.New(ctx).SeqErr(tolerant(ctx).Seq()).Collect()
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
