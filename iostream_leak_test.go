package chunkflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestMain fails the whole package if any test leaves a goroutine behind.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// blockUntilCancelled is a worker that never finishes on its own; it must be
// released by context cancellation.
func blockUntilCancelled[T any](ctx context.Context, item T) (T, error) {
	<-ctx.Done()
	return item, ctx.Err()
}

func TestIoStream_NoGoroutineLeaks(t *testing.T) {
	ctx := t.Context()

	t.Run("consumer short-circuits with Take after parallel MapCtx on infinite source", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Numbers(0)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				return i * 2, nil
			}, chunkflow.WithParallel(4)).
			Take(1).
			Collect()

		require.NoError(t, err)
		assert.Len(t, res, 1)
	})

	t.Run("consumer short-circuits after parallel FilterCtx on infinite source", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		val, ok, err := chunkflow.
			NewIo(ctx).Seq(seq.Numbers(0)).
			FilterCtx(func(_ context.Context, i int) (bool, error) {
				return i%7 == 0, nil
			}, chunkflow.WithParallel(4)).
			Skip(1).
			First()

		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 0, val%7)
	})

	t.Run("context cancelled while workers are blocked", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		cctx, cancel := context.WithCancel(ctx)
		var started atomic.Int32

		done := make(chan error, 1)
		go func() {
			_, err := chunkflow.
				NewIo(cctx).Seq(seq.Numbers(0)).
				MapCtx(func(ctx context.Context, i int) (int, error) {
					started.Add(1)
					return blockUntilCancelled(ctx, i)
				}, chunkflow.WithParallel(4)).
				Collect()
			done <- err
		}()

		// Wait until every worker is parked inside mapFn, then pull the plug.
		require.Eventually(t, func() bool { return started.Load() >= 4 }, 2*time.Second, time.Millisecond)
		cancel()

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("pipeline did not terminate after cancel")
		}
	})

	t.Run("one worker fails while others are still running", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		boom := errors.New("boom")
		_, err := chunkflow.
			NewIo(ctx).Seq(seq.Numbers(0)).
			MapCtx(func(ctx context.Context, i int) (int, error) {
				if i == 3 {
					return 0, boom
				}
				// Everyone else hangs until the failure cancels the stage.
				return blockUntilCancelled(ctx, i)
			}, chunkflow.WithParallel(4)).
			Collect()

		require.ErrorIs(t, err, boom)
	})

	t.Run("upstream error arrives while workers are busy", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		boom := errors.New("upstream")
		src := func(yield func(int, error) bool) {
			for i := range 8 {
				if !yield(i, nil) {
					return
				}
			}
			yield(0, boom)
		}

		_, err := chunkflow.
			NewIo(ctx).Seq2(src).
			MapCtx(blockUntilCancelled[int], chunkflow.WithParallel(4)).
			Collect()

		require.ErrorIs(t, err, boom)
	})

	t.Run("chained parallel stages short-circuited downstream", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		res, err := chunkflow.
			NewIo(ctx).Seq(seq.Numbers(0)).
			MapCtx(func(_ context.Context, i int) (int, error) { return i + 1, nil }, chunkflow.WithParallel(3)).
			FilterCtx(func(_ context.Context, i int) (bool, error) { return i%2 == 0, nil }, chunkflow.WithParallel(2)).
			Chunk[[]int](5).
			Through(chunkflow.IoFlatten).
			Take(10).
			Collect()

		require.NoError(t, err)
		assert.Len(t, res, 10)
	})
}
