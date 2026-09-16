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

func TestIoBuilder(t *testing.T) {
	ctx := t.Context()

	t.Run("options given to New become pipeline defaults", func(t *testing.T) {
		const workers = 4
		var entered atomic.Int32
		var timedOut atomic.Bool
		gate := make(chan struct{})
		go func() {
			defer close(gate)
			deadline := time.After(2 * time.Second)
			for entered.Load() < workers {
				select {
				case <-deadline:
					timedOut.Store(true)
					return
				default:
					time.Sleep(time.Millisecond)
				}
			}
		}()

		res, err := chunkflow.New(ctx, chunkflow.WithParallel(workers)).
			Seq(seq.Range(0, workers)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				entered.Add(1)
				<-gate
				return i, nil
			}).
			Collect()
		require.NoError(t, err)
		assert.ElementsMatch(t, []int{0, 1, 2, 3}, res)
		assert.False(t, timedOut.Load(), "WithParallel from New was not applied to MapCtx")
	})

	t.Run("the same builder can produce streams of different types", func(t *testing.T) {
		b := chunkflow.New(ctx)
		ints, err := b.Seq(seq.Items(1, 2)).Collect()
		require.NoError(t, err)
		strs, err := b.Seq(seq.Items("a")).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2}, ints)
		assert.Equal(t, []string{"a"}, strs)
	})

	t.Run("Seq attaches the context error once cancelled", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := chunkflow.New(cctx).Seq(seq.Numbers(0)).Take(1).Collect()
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Seq2 keeps source errors and adds the context error to clean elements", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := chunkflow.New(cctx).Seq2(errAfter(3, errBoom)).Collect()
		require.ErrorIs(t, err, context.Canceled, "the first element is clean at the source but the context is gone")

		_, err = chunkflow.New(ctx).Seq2(errAfter(3, errBoom)).Collect()
		require.ErrorIs(t, err, errBoom)
	})
}

func TestIoBuilder_Chan(t *testing.T) {
	ctx := t.Context()

	t.Run("reads until the channel is closed", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ch := make(chan int)
		go func() {
			defer close(ch)
			for i := range 5 {
				ch <- i
			}
		}()
		res, err := chunkflow.New(ctx).Chan(ch).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 2, 3, 4}, res)
	})

	t.Run("a closed empty channel is an empty stream", func(t *testing.T) {
		ch := make(chan int)
		close(ch)
		res, err := chunkflow.New(ctx).Chan(ch).Collect()
		require.NoError(t, err)
		assert.Empty(t, res)
	})

	t.Run("cancellation unblocks a receive on an empty channel", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		cctx, cancel := context.WithCancel(ctx)
		ch := make(chan int) // nobody ever writes

		done := make(chan error, 1)
		go func() {
			_, err := chunkflow.New(cctx).Chan(ch).Collect()
			done <- err
		}()
		time.Sleep(10 * time.Millisecond) // let Collect block on <-ch
		cancel()

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("Chan did not observe the cancellation")
		}
	})

	t.Run("values received before cancellation are kept", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		ch := make(chan int, 3)
		ch <- 1
		ch <- 2
		var got []int
		err := chunkflow.New(cctx).Chan(ch).ForEach(func(i int) {
			got = append(got, i)
			if i == 2 {
				cancel()
			}
		})
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, []int{1, 2}, got)
	})

	t.Run("stopping early stops reading but leaves the channel alone", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ch := make(chan int, 5)
		for i := range 5 {
			ch <- i
		}
		res, err := chunkflow.New(ctx).Chan(ch).Take(2).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1}, res)
		assert.Len(t, ch, 3, "unread items stay in the channel; the channel is not closed or drained")
	})

	t.Run("the stream is single-use", func(t *testing.T) {
		ch := make(chan int, 4)
		for i := range 4 {
			ch <- i
		}
		close(ch)
		s := chunkflow.New(ctx).Chan(ch)
		first, err := s.Take(3).Collect()
		require.NoError(t, err)
		second, err := s.Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 2}, first)
		assert.Equal(t, []int{3}, second, "the second pass gets what the first left behind")
	})

	t.Run("works with the worker pool on a producer that watches the context", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()

		jobs := make(chan int)
		go func() {
			defer close(jobs)
			for i := 0; ; i++ {
				select {
				case jobs <- i:
				case <-cctx.Done():
					return
				}
			}
		}()

		res, err := chunkflow.New(cctx, chunkflow.WithParallel(3)).
			Chan(jobs).
			MapCtx(func(_ context.Context, i int) (int, error) { return i * 2, nil }).
			Take(10).
			Collect()
		require.NoError(t, err)
		assert.Len(t, res, 10)
		cancel() // release the producer; goleak checks it actually exits
		time.Sleep(5 * time.Millisecond)
	})
}
