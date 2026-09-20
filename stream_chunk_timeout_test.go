package chunkflow_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/policy"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// trickle sends the given bursts into a channel with a pause between them, then closes it.
func trickle(pause time.Duration, bursts ...[]int) <-chan int {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i, burst := range bursts {
			if i > 0 {
				time.Sleep(pause)
			}
			for _, v := range burst {
				ch <- v
			}
		}
	}()
	return ch
}

func TestStream_ChunkTimeout(t *testing.T) {
	ctx := t.Context()

	t.Run("behaves like Chunk on a source that never pauses", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 7)).ChunkTimeout[[]int](3, time.Hour).Collect()
		require.NoError(t, err)
		assert.Equal(t, [][]int{{0, 1, 2}, {3, 4, 5}, {6}}, res)
	})

	t.Run("a partial chunk is released by the timeout, not by the next value", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		start := time.Now()
		var at []time.Duration
		res, err := chunkflow.
			New(ctx).
			Chan(trickle(300*time.Millisecond, []int{1, 2, 3}, []int{4, 5})).
			ChunkTimeout(10, 30*time.Millisecond).
			Tap(func([]int) { at = append(at, time.Since(start)) }).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, [][]int{{1, 2, 3}, {4, 5}}, res)
		require.Len(t, at, 2)
		assert.Less(t, at[0], 200*time.Millisecond, "the first chunk must not wait for the second burst")
		assert.GreaterOrEqual(t, at[0], 30*time.Millisecond)
	})

	t.Run("the clock starts with the first value: an idle source emits nothing", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ch := make(chan int)
		go func() {
			time.Sleep(120 * time.Millisecond) // idle for four timeouts
			ch <- 1
			close(ch)
		}()
		start := time.Now()
		res, err := chunkflow.New(ctx).Chan(ch).ChunkTimeout[[]int](10, 30*time.Millisecond).Collect()
		require.NoError(t, err)
		assert.Equal(t, [][]int{{1}}, res, "no empty chunks while waiting")
		assert.GreaterOrEqual(t, time.Since(start), 120*time.Millisecond)
	})

	t.Run("a full chunk resets the clock", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		// six fast values with size 2: three full chunks, the timer must never fire
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 6)).ChunkTimeout[[]int](2, 10*time.Millisecond).Collect()
		require.NoError(t, err)
		assert.Equal(t, [][]int{{0, 1}, {2, 3}, {4, 5}}, res)
	})

	t.Run("errors pass through immediately and the partial chunk is kept", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		src := func(yield func(int, error) bool) {
			_ = yield(1, nil) && yield(2, nil) && yield(0, errBoom) && yield(3, nil)
		}
		var got []string
		for v, err := range chunkflow.New(ctx).Seq2(src).ChunkTimeout[[]int](5, time.Hour).
			Through(policy.CircuitBreaker[[]int](100)).Seq() {
			if err != nil {
				got = append(got, "err")
				continue
			}
			got = append(got, fmt.Sprint(v))
		}
		assert.Equal(t, []string{"err", "[1 2 3]"}, got)
	})

	t.Run("a fatal error ends the stream", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		res, err := chunkflow.New(ctx).Seq2(errAfter(3, errBoom)).ChunkTimeout[[]int](2, time.Hour).Collect()
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, [][]int{{0, 1}}, res, "the partial chunk [2] is lost with the fatal error, as with Chunk")
	})

	t.Run("invalid arguments are an error stream", func(t *testing.T) {
		_, err := chunkflow.New(ctx).Seq(seq.Items(1)).ChunkTimeout[[]int](0, time.Second).Collect()
		require.ErrorContains(t, err, "ChunkTimeout(0, 1s): size must be at least 1")
		_, err = chunkflow.New(ctx).Seq(seq.Items(1)).ChunkTimeout[[]int](2, 0).Collect()
		require.ErrorContains(t, err, "maxWait must be positive")
	})

	t.Run("Take on an infinite source stops the feeder", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		res, err := chunkflow.New(ctx).Seq(seq.Numbers(0)).ChunkTimeout[[]int](3, time.Hour).Take(2).Collect()
		require.NoError(t, err)
		assert.Equal(t, [][]int{{0, 1, 2}, {3, 4, 5}}, res)
	})

	t.Run("the consumer may stop on the final flush or on a timeout flush", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		// final flush: the only chunk is the partial one at the end of the source
		res, err := chunkflow.New(ctx).Seq(seq.Items(1, 2)).ChunkTimeout[[]int](5, time.Hour).Take(1).Collect()
		require.NoError(t, err)
		assert.Equal(t, [][]int{{1, 2}}, res)

		// timeout flush: the first chunk is released by the clock and First stops right there
		first, err := chunkflow.New(ctx).Chan(trickle(300*time.Millisecond, []int{1, 2}, []int{3})).
			ChunkTimeout[[]int](10, 30*time.Millisecond).First()
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2}, first)
	})

	t.Run("works behind a parallel stage and in front of Take", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		res, err := chunkflow.New(ctx).Seq(seq.Numbers(0)).
			MapCtx(func(_ context.Context, i int) (int, error) { return i * 2, nil }, chunkflow.WithParallel(4)).
			ChunkTimeout[[]int](2, time.Hour).
			Take(2).Collect()
		require.NoError(t, err)
		assert.Equal(t, [][]int{{0, 2}, {4, 6}}, res)
	})

	t.Run("cancellation while waiting on a silent channel ends with the context error", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		cctx, cancel := context.WithCancel(ctx)
		ch := make(chan int) // nobody ever sends
		done := make(chan error, 1)
		go func() {
			_, err := chunkflow.New(cctx).Chan(ch).ChunkTimeout[[]int](10, time.Hour).Collect()
			done <- err
		}()
		time.Sleep(20 * time.Millisecond)
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("pipeline did not end after cancel")
		}
	})

	t.Run("a source that ends silently after cancellation still reports the context error", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		cctx, cancel := context.WithCancel(ctx)
		silent := func(yield func(int, error) bool) { <-cctx.Done() } // ends without yielding anything
		go func() { time.Sleep(20 * time.Millisecond); cancel() }()
		_, err := chunkflow.New(cctx).Seq2(silent).ChunkTimeout[[]int](10, time.Hour).Collect()
		require.ErrorIs(t, err, context.Canceled, "the feeder exited without emitting; the operator itself must say so")
	})

}
