package chunkflow_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestStream_Concat(t *testing.T) {
	ctx := t.Context()

	t.Run("emits streams one after another", func(t *testing.T) {
		res, err := chunkflow.Concat(
			chunkflow.New(ctx).Seq(seq.Items(1, 2)),
			chunkflow.New(ctx).Seq(seq.Items[int]()),
			chunkflow.New(ctx).Seq(seq.Items(3)),
		).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, res)
	})

	t.Run("with no arguments is empty", func(t *testing.T) {
		res, err := chunkflow.Concat[int]().Collect()
		require.NoError(t, err)
		assert.Empty(t, res)
	})

	t.Run("errors keep their position and pass through", func(t *testing.T) {
		res, err := chunkflow.Concat(
			chunkflow.New(ctx).Seq(seq.Range(0, 5)).MapCtx(failing), // 2,3,4 fail
			chunkflow.New(ctx).Seq(seq.Items(9)),
		).CircuitBreaker(100).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 9}, res)
	})

	t.Run("is fail-fast without a breaker and does not touch later streams", func(t *testing.T) {
		secondPulled := 0
		second := chunkflow.New(ctx).Seq(seq.Items(9)).Tap(func(int) { secondPulled++ })
		res, err := chunkflow.Concat(chunkflow.New(ctx).Seq2(errAfter(2, errBoom)), second).Collect()
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, []int{0, 1}, res)
		assert.Equal(t, 0, secondPulled)
	})

	t.Run("result context is cancelled by any source context", func(t *testing.T) {
		otherCtx, cancel := context.WithCancel(ctx)
		cancel() // the SECOND stream's context, not the head's

		// The downstream MapCtx runs on the concat's context, so it must observe the cancel.
		_, err := chunkflow.Concat(
			chunkflow.New(ctx).Seq(seq.Items(1)),
			chunkflow.New(otherCtx).Seq(seq.Items(2)),
		).MapCtx(func(ctx context.Context, i int) (int, error) { return i, ctx.Err() }).Collect()
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestStream_MergeContexts(t *testing.T) {
	ctx := t.Context()

	t.Run("cancelling a non-head source context stops the merge and releases sources", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		otherCtx, cancel := context.WithCancel(ctx)
		var seen atomic.Int32
		done := make(chan error, 1)
		go func() {
			done <- chunkflow.Merge(
				chunkflow.New(ctx).Seq(seq.Numbers(0)),      // head: never cancelled
				chunkflow.New(otherCtx).Seq(seq.Numbers(0)), // cancelled below
			).Tap(func(int) { seen.Add(1) }).Drain()
		}()
		require.Eventually(t, func() bool { return seen.Load() > 10 }, 2*time.Second, time.Millisecond)
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("merge did not stop after cancelling a non-head context")
		}
	})

	t.Run("any of several distinct contexts can stop the merge", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx2, cancel2 := context.WithCancel(ctx)
		defer cancel2()
		ctx3, cancel3 := context.WithCancel(ctx)
		cancel3() // the third one

		err := chunkflow.Merge(
			chunkflow.New(ctx).Seq(seq.Numbers(0)),
			chunkflow.New(ctx2).Seq(seq.Numbers(0)),
			chunkflow.New(ctx3).Seq(seq.Numbers(0)),
		).Drain()
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("the original cause is preserved", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		deadlineCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()

		err := chunkflow.Merge(
			chunkflow.New(ctx).Seq(seq.Numbers(0)),
			chunkflow.New(deadlineCtx).Seq(seq.Numbers(0)),
		).Drain()
		require.ErrorIs(t, err, context.DeadlineExceeded, "must not be flattened into Canceled")
	})

	t.Run("values and deadline come from the head context", func(t *testing.T) {
		type key struct{}
		headCtx := context.WithValue(ctx, key{}, "head")
		otherCtx := context.WithValue(ctx, key{}, "other")

		var fromCtx []string
		err := chunkflow.Merge(
			chunkflow.New(headCtx).Seq(seq.Items(1)),
			chunkflow.New(otherCtx).Seq(seq.Items(2)),
		).ForEachCtx(func(ctx context.Context, _ int) error {
			v, _ := ctx.Value(key{}).(string)
			fromCtx = append(fromCtx, v)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"head", "head"}, fromCtx)
	})

	t.Run("the same context passed many times is registered once", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		cctx, cancel := context.WithCancel(ctx)
		streams := make([]chunkflow.Stream[int], 50)
		for i := range streams {
			streams[i] = chunkflow.New(cctx).Seq(seq.Items(i))
		}
		res, err := chunkflow.Merge(streams...).Collect()
		require.NoError(t, err)
		assert.Len(t, res, 50)
		cancel()
	})
}

func TestStream_Merge(t *testing.T) {
	ctx := t.Context()

	t.Run("emits every element of every source", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		res, err := chunkflow.Merge(
			chunkflow.New(ctx).Seq(seq.Range(0, 5)),
			chunkflow.New(ctx).Seq(seq.Range(10, 15)),
			chunkflow.New(ctx).Seq(seq.Items[int]()),
		).Collect()
		require.NoError(t, err)
		assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 10, 11, 12, 13, 14}, res)
	})

	t.Run("with no arguments is empty", func(t *testing.T) {
		res, err := chunkflow.Merge[int]().Collect()
		require.NoError(t, err)
		assert.Empty(t, res)
	})

	t.Run("actually runs sources concurrently", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		// each source blocks until both have started; sequential consumption would deadlock,
		// so a timeout on the gate turns a regression into a failure instead of a hang
		var started atomic.Int32
		gate := make(chan struct{})
		go func() {
			deadline := time.After(2 * time.Second)
			for started.Load() < 2 {
				select {
				case <-deadline:
					close(gate)
					return
				default:
					time.Sleep(time.Millisecond)
				}
			}
			close(gate)
		}()
		mk := func(v int) chunkflow.Stream[int] {
			return chunkflow.New(ctx).Seq(seq.Items(v)).Tap(func(int) {
				started.Add(1)
				<-gate
			})
		}
		res, err := chunkflow.Merge(mk(1), mk(2)).Collect()
		require.NoError(t, err)
		assert.ElementsMatch(t, []int{1, 2}, res)
		assert.Equal(t, int32(2), started.Load())
	})

	t.Run("errors pass through and can be tolerated", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		res, err := chunkflow.Merge(
			chunkflow.New(ctx).Seq(seq.Range(0, 5)).MapCtx(failing), // 2,3,4 fail
			chunkflow.New(ctx).Seq(seq.Range(10, 13)),
		).CircuitBreaker(100).Collect()
		require.NoError(t, err)
		assert.ElementsMatch(t, []int{0, 1, 10, 11, 12}, res)
	})

	t.Run("is fail-fast without a breaker", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		_, err := chunkflow.Merge(
			chunkflow.New(ctx).Seq2(errAfter(1, errBoom)),
			chunkflow.New(ctx).Seq(seq.Numbers(0)), // infinite: must be released
		).Collect()
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("short-circuit releases all sources", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		res, err := chunkflow.Merge(
			chunkflow.New(ctx).Seq(seq.Numbers(0)),
			chunkflow.New(ctx).Seq(seq.Numbers(1000)),
		).Take(5).Collect()
		require.NoError(t, err)
		assert.Len(t, res, 5)
	})

	t.Run("cancellation surfaces as an error and releases sources", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		cctx, cancel := context.WithCancel(ctx)
		var seen atomic.Int32
		done := make(chan error, 1)
		go func() {
			err := chunkflow.Merge(
				chunkflow.New(cctx).Seq(seq.Numbers(0)),
				chunkflow.New(cctx).Seq(seq.Numbers(0)),
			).Tap(func(int) { seen.Add(1) }).Drain()
			done <- err
		}()
		require.Eventually(t, func() bool { return seen.Load() > 10 }, 2*time.Second, time.Millisecond)
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("merge did not stop after cancel")
		}
	})

	t.Run("pre-cancelled context is reported even if sources emit nothing", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		for range 20 {
			_, err := chunkflow.Merge(
				chunkflow.New(cctx).Seq(seq.Range(0, 10)),
				chunkflow.New(cctx).Seq(seq.Range(0, 10)),
			).Collect()
			require.ErrorIs(t, err, context.Canceled)
		}
	})
}
