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

func TestIoStream_Basic(t *testing.T) {
	ctx := t.Context()

	t.Run("NewIo().Seq and Collect success", func(t *testing.T) {
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 2, 3)).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, res)
	})

	t.Run("TryMap success and failure", func(t *testing.T) {
		// Success case
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 2)).
			MapCtx(func(ctx context.Context, item int) (int, error) {
				return item * 10, nil
			}).
			Collect()

		require.NoError(t, err)
		assert.Equal(t, []int{10, 20}, res)

		// Failure case
		res, err = chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 2)).
			MapCtx(func(ctx context.Context, item int) (int, error) {
				if item == 2 {
					return 0, errors.New("test error")
				}
				return item * 10, nil
			}).
			Collect()

		require.ErrorContains(t, err, "test error")
		assert.Equal(t, []int{10}, res) // 2 is not collected as it returned error
	})
}

func TestIoStream_ConcurrentMap(t *testing.T) {
	ctx := t.Context()

	t.Run("concurrency less than 1 propagates error", func(t *testing.T) {
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1)).
			MapCtx(func(ctx context.Context, item int) (int, error) {
				return item, nil
			}, chunkflow.WithParallel(0)).
			Collect()

		require.ErrorContains(t, err, "concurrency must be at least 1")
		assert.Empty(t, res)
	})

	t.Run("valid ConcurrentMap transforms elements", func(t *testing.T) {
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 2, 3)).
			MapCtx(func(ctx context.Context, item int) (int, error) {
				return item * 2, nil
			}, chunkflow.WithParallel(2)).
			Collect()

		require.NoError(t, err)
		assert.ElementsMatch(t, []int{2, 4, 6}, res)
	})

	t.Run("MapCtx returns error from worker", func(t *testing.T) {
		var dummyErr = errors.New("concurrent error")

		_, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 2)).
			MapCtx(func(ctx context.Context, item int) (int, error) {
				if item == 2 {
					return 0, dummyErr
				}
				return item, nil
			}, chunkflow.WithParallel(2)).
			Collect()

		assert.ErrorIs(t, err, dummyErr)
	})
}

func TestIoStream_Take(t *testing.T) {
	ctx := t.Context()

	t.Run("Take exact number of elements", func(t *testing.T) {
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(10, 20, 30)).
			Take(2).
			Collect()

		require.NoError(t, err)
		assert.Equal(t, []int{10, 20}, res)
	})

	t.Run("Take non-positive count returns empty", func(t *testing.T) {
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(10)).
			Take(0).
			Collect()
		require.NoError(t, err)
		assert.Empty(t, res)
	})
}

func TestIoStream_CtxMethods(t *testing.T) {
	ctx := t.Context()

	t.Run("FilterCtx", func(t *testing.T) {
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 2, 3, 4)).
			FilterCtx(func(_ context.Context, i int) (bool, error) {
				return i%2 == 0, nil
			}).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{2, 4}, res)
	})

	t.Run("Chunk", func(t *testing.T) {
		chunks, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 2, 3, 4, 5)).
			Chunk(2).
			Collect()

		require.NoError(t, err)
		assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, chunks)
	})

	t.Run("Skip", func(t *testing.T) {
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 2, 3, 4)).
			Skip(2).
			Collect()

		require.NoError(t, err)
		assert.Equal(t, []int{3, 4}, res)
	})

	t.Run("ReduceCtx", func(t *testing.T) {
		sum, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 2, 3)).
			ReduceCtx(0, func(ctx context.Context, acc, item int) (int, error) {
				return acc + item, nil
			})
		require.NoError(t, err)
		assert.Equal(t, 6, sum)
	})

	t.Run("AllCtx", func(t *testing.T) {

		allEven, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(2, 4, 6)).
			AllCtx(func(ctx context.Context, i int) (bool, error) {
				return i%2 == 0, nil
			})

		require.NoError(t, err)
		assert.True(t, allEven)
	})

	t.Run("All", func(t *testing.T) {

		allEven, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(2, 4, 6)).
			All(func(item int) bool {
				return item > 0
			})

		require.NoError(t, err)
		assert.True(t, allEven)
	})

	t.Run("FilterCtx parallel", func(t *testing.T) {
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 2, 3, 4)).
			FilterCtx(func(ctx context.Context, i int) (bool, error) {
				return i%2 == 0, nil
			}, chunkflow.WithParallel(2)).
			Collect()
		require.NoError(t, err)
		assert.ElementsMatch(t, []int{2, 4}, res)
	})

	t.Run("TryCollect", func(t *testing.T) {
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Range(1, 4)).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, res)
	})

	t.Run("Through", func(t *testing.T) {
		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Numbers(0)).
			Through(func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] {
				return s.Skip(2).Take(1)
			}).
			Collect()

		require.NoError(t, err)
		assert.Equal(t, []int{2}, res)
	})

	t.Run("AnyCtx", func(t *testing.T) {

		hasEven, err := chunkflow.
			NewIo(ctx).Seq(seq.Items(1, 3, 4)).
			AnyCtx(func(ctx context.Context, i int) (bool, error) {
				return i%2 == 0, nil
			})
		require.NoError(t, err)
		assert.True(t, hasEven)
	})
}
