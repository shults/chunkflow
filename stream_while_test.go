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

func TestStream_TakeWhile(t *testing.T) {
	ctx := t.Context()
	lt4 := func(i int) bool { return i < 4 }

	t.Run("stops at the first mismatch and stops pulling", func(t *testing.T) {
		pulled := 0
		res, err := chunkflow.New(ctx).Seq(seq.Items(1, 2, 3, 10, 4, 5)).
			Tap(func(int) { pulled++ }).
			TakeWhile(lt4).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, res)
		assert.Equal(t, 4, pulled)
	})

	t.Run("errors pass through without being evaluated", func(t *testing.T) {
		// 0..9 with 2,3,4 failing; predicate i < 6 must only see values
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(failing).
			TakeWhile(func(i int) bool { return i < 6 }).
			Through(policy.CircuitBreaker[int](100)).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5}, res)
	})

	t.Run("does not see errors after it has stopped", func(t *testing.T) {
		src := func(yield func(int, error) bool) {
			for _, v := range []int{1, 2, 3} {
				if !yield(v, nil) {
					return
				}
			}
			yield(0, errBoom) // never reached: TakeWhile stops on 3
		}
		res, err := chunkflow.New(ctx).SeqErr(src).TakeWhile(func(i int) bool { return i < 3 }).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2}, res)
	})

	t.Run("Ctx predicate error is fatal without a breaker", func(t *testing.T) {
		errPred := errors.New("predicate")
		res, err := chunkflow.New(ctx).Seq(seq.Numbers(0)).
			TakeWhileCtx(func(_ context.Context, i int) (bool, error) {
				if i == 2 {
					return false, errPred
				}
				return true, nil
			}).
			Collect()
		require.ErrorIs(t, err, errPred)
		assert.Equal(t, []int{0, 1}, res)
	})

	t.Run("Ctx predicate error is emitted and evaluation continues", func(t *testing.T) {
		errPred := errors.New("predicate")
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 10)).
			TakeWhileCtx(func(_ context.Context, i int) (bool, error) {
				if i == 2 {
					return false, errPred // must not end the stream on its own
				}
				return i < 5, nil
			}).
			Through(policy.CircuitBreaker[int](100)).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 3, 4}, res)
	})

	t.Run("Ctx predicate receives the stream context", func(t *testing.T) {
		type key struct{}
		cctx := context.WithValue(ctx, key{}, true)
		res, err := chunkflow.New(cctx).Seq(seq.Range(0, 3)).
			TakeWhileCtx(func(ctx context.Context, _ int) (bool, error) {
				v, _ := ctx.Value(key{}).(bool)
				return v, nil
			}).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 2}, res)
	})
}

func TestStream_SkipWhile(t *testing.T) {
	ctx := t.Context()
	lt4 := func(i int) bool { return i < 4 }

	t.Run("drops the prefix and then stops evaluating", func(t *testing.T) {
		evaluated := 0
		res, err := chunkflow.New(ctx).Seq(seq.Items(1, 2, 3, 10, 4, 5)).
			SkipWhile(func(i int) bool { evaluated++; return lt4(i) }).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{10, 4, 5}, res)
		assert.Equal(t, 4, evaluated)
	})

	t.Run("errors pass through in both phases", func(t *testing.T) {
		// 0..9 with 2,3,4 failing; skip while i < 6 -> errors at 2,3,4 are emitted during skipping
		var suppressed int
		var values []int
		for v, err := range chunkflow.New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(failing).
			SkipWhile(func(i int) bool { return i < 6 }).
			Through(policy.CircuitBreaker[int](100)).
			Seq() {
			if err != nil {
				require.ErrorIs(t, err, chunkflow.ErrSuppressed)
				suppressed++
				continue
			}
			values = append(values, v)
		}
		assert.Equal(t, []int{6, 7, 8, 9}, values)
		assert.Equal(t, 3, suppressed, "errors are not swallowed by the skipping phase")
	})

	t.Run("Ctx predicate error is emitted and skipping continues", func(t *testing.T) {
		errPred := errors.New("predicate")
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 8)).
			SkipWhileCtx(func(_ context.Context, i int) (bool, error) {
				if i == 1 {
					return false, errPred
				}
				return i < 4, nil
			}).
			Through(policy.CircuitBreaker[int](100)).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{4, 5, 6, 7}, res)
	})

	t.Run("stops pulling once the consumer is done", func(t *testing.T) {
		pulled := 0
		v, err := chunkflow.New(ctx).Seq(seq.Numbers(0)).
			Tap(func(int) { pulled++ }).
			SkipWhile(lt4).
			First()
		require.NoError(t, err)
		assert.Equal(t, 4, v)
		assert.Equal(t, 5, pulled)
	})

	t.Run("Ctx predicate error is fatal without a breaker", func(t *testing.T) {
		errPred := errors.New("predicate")
		_, err := chunkflow.New(ctx).Seq(seq.Range(0, 8)).
			SkipWhileCtx(func(context.Context, int) (bool, error) { return false, errPred }).
			Collect()
		require.ErrorIs(t, err, errPred)
	})
}
