package chunkflow_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/policy"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func pairStr(a int, b string) string { return fmt.Sprintf("%d%s", a, b) }

func TestStream_Zip(t *testing.T) {
	ctx := t.Context()
	letters := func() chunkflow.Stream[string] { return chunkflow.New(ctx).Seq(seq.Items("a", "b", "c")) }

	t.Run("pairs positionally and ends at the shorter side, either side", func(t *testing.T) {
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 5)).Zip(letters(), pairStr).Collect()
		require.NoError(t, err)
		assert.Equal(t, []string{"0a", "1b", "2c"}, res)

		res, err = chunkflow.New(ctx).Seq(seq.Range(0, 2)).Zip(letters(), pairStr).Collect()
		require.NoError(t, err)
		assert.Equal(t, []string{"0a", "1b"}, res)
	})

	t.Run("stops pulling both sides when the shorter one ends", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		var left, right atomic.Int32
		res, err := chunkflow.
			New(ctx).
			Seq(seq.Numbers(0)).Tap(func(int) { left.Add(1) }).
			Zip(letters().Tap(func(string) { right.Add(1) }), pairStr).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []string{"0a", "1b", "2c"}, res)
		assert.Equal(t, int32(3), right.Load())
		assert.Equal(t, int32(4), left.Load(), "the element pulled while discovering the end is dropped")
	})

	t.Run("an error on the left is forwarded without consuming a value on the right", func(t *testing.T) {
		var right atomic.Int32
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 5)).MapCtx(failing). // 2,3,4 fail
											Zip(letters().Tap(func(string) { right.Add(1) }), pairStr).
											Through(policy.CircuitBreaker[string](100)).
											Collect()
		require.NoError(t, err)
		assert.Equal(t, []string{"0a", "1b"}, res, "left values 0,1 pair with a,b; 2,3,4 are errors; the left ends")
		assert.Equal(t, int32(2), right.Load(), "c was never pulled")
	})

	t.Run("an error on the right is forwarded and the left value waits for the next right value", func(t *testing.T) {
		right := chunkflow.New(ctx).SeqErr(func(yield func(string, error) bool) {
			_ = yield("a", nil) && yield("", errBoom) && yield("b", nil)
		})
		var got []string
		for v, err := range chunkflow.New(ctx).Seq(seq.Range(0, 5)).Zip(right, pairStr).Seq() {
			if err != nil {
				got = append(got, "err")
				if !errors.Is(err, errBoom) {
					t.Fatalf("unexpected %v", err)
				}
				break // fatal for a consumer; we only record the order
			}
			got = append(got, v)
		}
		assert.Equal(t, []string{"0a", "err"}, got)

		// tolerated, the pipeline goes on: 1 pairs with b
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 5)).Zip(right, pairStr).
			Through(policy.CircuitBreaker[string](100)).Collect()
		require.NoError(t, err)
		assert.Equal(t, []string{"0a", "1b"}, res)
	})

	t.Run("fatal error stops the pipeline at its position", func(t *testing.T) {
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 5)).MapCtx(failing).Zip(letters(), pairStr).Collect()
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, []string{"0a", "1b"}, res)
	})

	t.Run("the same stream can be zipped with itself", func(t *testing.T) {
		src := chunkflow.New(ctx).Seq(seq.Range(0, 3))
		res, err := src.Zip(src, func(a, b int) int { return a + b }).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 2, 4}, res)
	})

	t.Run("three sources by nesting into a named struct", func(t *testing.T) {
		type row struct {
			n    int
			s    string
			flag bool
		}
		flags := chunkflow.New(ctx).Seq(seq.Items(true, false, true))
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 3)).
			Zip(letters(), func(n int, s string) row { return row{n: n, s: s} }).
			Zip(flags, func(r row, f bool) row { r.flag = f; return r }).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []row{{0, "a", true}, {1, "b", false}, {2, "c", true}}, res)
	})

	t.Run("a panic in fn becomes ErrPanic at its position", func(t *testing.T) {
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 3)).Zip(letters(), func(a int, b string) string {
			if a == 1 {
				panic("bug")
			}
			return pairStr(a, b)
		}).Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
		assert.Equal(t, []string{"0a"}, res)
	})

	t.Run("Take after Zip on two infinite sources is leak-free", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		res, err := chunkflow.New(ctx).Seq(seq.Numbers(0)).
			Zip(chunkflow.New(ctx).Seq(seq.Numbers(100)), func(a, b int) int { return a + b }).
			Take(3).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{100, 102, 104}, res)
	})

	t.Run("a parallel stage on the pulled side is stopped with the pipeline", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		right := chunkflow.New(ctx).Seq(seq.Numbers(0)).
			MapCtx(func(_ context.Context, i int) (int, error) { return i * 10, nil }, chunkflow.WithParallel(4))
		res, err := chunkflow.New(ctx).Seq(seq.Numbers(0)).Zip(right, func(a, b int) int { return a + b }).Take(3).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 11, 22}, res)
	})

	t.Run("cancelling the other side's context ends the pipeline with that cause", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		octx, cancel := context.WithCancelCause(ctx)
		right := chunkflow.New(octx).Seq(seq.Numbers(0))
		n := 0
		_, err := chunkflow.New(ctx).Seq(seq.Numbers(0)).
			ZipCtx(right, func(ctx context.Context, a, b int) (int, error) {
				n++
				if n == 3 {
					cancel(errBoom)
				}
				return a + b, ctx.Err()
			}).
			Collect()
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 3, n)
	})

	t.Run("options come from the left stream", func(t *testing.T) {
		var seen []error
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 4)).
			Opts(chunkflow.WithOnError(func(err error) { seen = append(seen, err) })).
			ZipCtx(letters(), func(_ context.Context, a int, b string) (string, error) {
				if a == 1 {
					return "", chunkflow.Suppress(errBoom)
				}
				return pairStr(a, b), nil
			}).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []string{"0a", "2c"}, res)
		require.Len(t, seen, 1)
		assert.ErrorIs(t, seen[0], errBoom)
	})

	t.Run("ZipCtx gets the merged context: values from the left, cancellation from the right", func(t *testing.T) {
		type key struct{}
		lctx := context.WithValue(ctx, key{}, "left")
		rctx, cancel := context.WithTimeout(ctx, time.Hour)
		defer cancel()
		var sawValue, sawDeadline bool
		err := chunkflow.New(lctx).Seq(seq.Range(0, 1)).
			ZipCtx(chunkflow.New(rctx).Seq(seq.Range(0, 1)), func(ctx context.Context, a, b int) (int, error) {
				sawValue = ctx.Value(key{}) == "left"
				_, sawDeadline = ctx.Deadline()
				return a + b, nil
			}).Drain()
		require.NoError(t, err)
		assert.True(t, sawValue)
		assert.False(t, sawDeadline, "deadline comes from the left context only, like Merge")
	})
}
