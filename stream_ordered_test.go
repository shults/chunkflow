package chunkflow_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// jitter sleeps for a random sub-millisecond duration so that workers finish out of order.
func jitter() { time.Sleep(time.Duration(rand.IntN(500)) * time.Microsecond) } //nolint:gosec // test jitter

func TestStream_Ordered(t *testing.T) {
	ctx := t.Context()
	const workers = 4
	const window = 2 * workers // readAhead(2) * WithParallel(4); not user-settable yet

	t.Run("MapCtx and FilterCtx preserve source order under random delays", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		want := make([]int, 0, 300)
		for i := range 300 {
			if i%3 != 0 {
				want = append(want, i*10)
			}
		}

		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 300)).
			FilterCtx(func(_ context.Context, i int) (bool, error) { jitter(); return i%3 != 0, nil }, chunkflow.WithParallel(workers)).
			MapCtx(func(_ context.Context, i int) (int, error) { jitter(); return i * 10, nil }, chunkflow.WithParallel(workers)).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, want, res, "not merely the same elements: the same order")
	})

	t.Run("read-ahead is bounded by the window while the head item is slow", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		var pulled, entered atomic.Int32
		release := make(chan struct{})
		releaseHead := sync.OnceFunc(func() { close(release) })
		defer releaseHead() // never leave the pipeline hanging, even when an assertion fails

		done := make(chan []int, 1)
		go func() {
			res, err := chunkflow.New(ctx).Seq(seq.Range(0, 50)).
				Tap(func(int) { pulled.Add(1) }).
				MapCtx(func(_ context.Context, i int) (int, error) {
					entered.Add(1)
					if i == 0 {
						<-release // head of the line, everything else waits behind it
					}
					return i, nil
				}, chunkflow.WithParallel(workers)).
				Collect()
			assert.NoError(t, err)
			done <- res
		}()

		// The free workers keep working through the window while the head blocks: every
		// item pulled so far is processed, then pulls stop, because nothing has been emitted.
		require.Eventually(t, func() bool { return entered.Load() == window }, 2*time.Second, time.Millisecond,
			"idle workers should still process the read-ahead while the head is blocked")
		require.Eventually(t, func() bool { return pulled.Load() == window }, 2*time.Second, time.Millisecond)
		time.Sleep(20 * time.Millisecond)
		assert.Equal(t, int32(window), pulled.Load(), "source pulled beyond the window while nothing was emitted")

		releaseHead()
		select {
		case res := <-done:
			assert.Len(t, res, 50)
			assert.IsIncreasing(t, res)
		case <-time.After(2 * time.Second):
			t.Fatal("pipeline did not finish after the head was released")
		}
	})

	t.Run("errors keep their position, so a downstream CircuitBreaker sees source order", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		boom := errors.New("boom")
		src := func(yield func(int, error) bool) {
			for i := range 10 {
				if i == 5 && !yield(0, boom) {
					return
				}
				if !yield(i, nil) {
					return
				}
			}
		}

		var got []string
		for v, err := range chunkflow.New(ctx).Seq2(src).
			MapCtx(func(_ context.Context, i int) (int, error) {
				jitter()
				if i == 7 {
					return 0, boom
				}
				return i, nil
			}, chunkflow.WithParallel(workers)).
			CircuitBreaker(100).
			Seq() {
			switch {
			case err == nil:
				got = append(got, string(rune('0'+v)))
			case errors.Is(err, boom):
				got = append(got, "err")
			default:
				t.Fatalf("unexpected error %v", err)
			}
		}
		assert.Equal(t, []string{"0", "1", "2", "3", "4", "err", "5", "6", "err", "8", "9"}, got)
	})

	t.Run("a panic surfaces at its position and drops the rest", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 20)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i == 2 {
					panic("bug")
				}
				return i, nil
			}, chunkflow.WithParallel(workers)).
			Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
		assert.Equal(t, []int{0, 1}, res, "elements before the panic are delivered in order, nothing after it")
	})

	t.Run("Take(1) after an ordered stage stops the pool and the source", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		var pulled atomic.Int32
		res, err := chunkflow.New(ctx).Seq(seq.Numbers(0)).
			Tap(func(int) { pulled.Add(1) }).
			MapCtx(func(_ context.Context, i int) (int, error) { return i, nil }, chunkflow.WithParallel(workers)).
			Take(1).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0}, res)
		assert.LessOrEqual(t, pulled.Load(), int32(window))
	})

	t.Run("cancelled context surfaces as an error", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		cctx, cancel := context.WithCancel(ctx)
		var started atomic.Int32
		done := make(chan error, 1)
		go func() {
			_, err := chunkflow.New(cctx).Seq(seq.Numbers(0)).
				MapCtx(func(ctx context.Context, i int) (int, error) {
					started.Add(1)
					return blockUntilCancelled(ctx, i)
				}, chunkflow.WithParallel(workers)).
				Collect()
			done <- err
		}()
		require.Eventually(t, func() bool { return started.Load() == workers }, 2*time.Second, time.Millisecond)
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("pipeline did not terminate after cancel")
		}
	})
}

func TestStream_Unordered(t *testing.T) {
	ctx := t.Context()

	t.Run("results overtake a slow predecessor", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		gate := make(chan struct{})
		var first int
		for v, err := range chunkflow.New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i == 0 {
					<-gate // released only once some other result has been emitted
				}
				return i, nil
			}, chunkflow.WithParallel(4), chunkflow.WithUnordered()).
			Seq() {
			require.NoError(t, err)
			first = v
			break
		}
		close(gate)
		assert.NotEqual(t, 0, first, "with ordering on this loop would deadlock; unordered emits whatever is ready")
	})

	t.Run("same elements, a panic is still fatal", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 20)).
			MapCtx(func(_ context.Context, i int) (int, error) { jitter(); return i, nil }, chunkflow.WithParallel(4), chunkflow.WithUnordered()).
			Collect()
		require.NoError(t, err)
		assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, res)

		_, err = chunkflow.New(ctx).Seq(seq.Range(0, 20)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i == 2 {
					panic("bug")
				}
				return i, nil
			}, chunkflow.WithParallel(4), chunkflow.WithUnordered()).
			Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
	})

	t.Run("Take(1) on an infinite source is leak-free", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		res, err := chunkflow.New(ctx).Seq(seq.Numbers(0)).
			MapCtx(func(_ context.Context, i int) (int, error) { return i, nil }, chunkflow.WithParallel(4), chunkflow.WithUnordered()).
			Take(1).
			Collect()
		require.NoError(t, err)
		assert.Len(t, res, 1)
	})

	t.Run("consumer leaves while the feeder is parked on a full output buffer", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		boom := errors.New("boom")
		var yielded atomic.Int32
		src := func(yield func(int, error) bool) {
			for {
				yielded.Add(1)
				if !yield(0, boom) {
					return
				}
			}
		}
		// Upstream errors bypass the workers and go straight to the output buffer (capacity 4).
		// One is consumed, four sit in the buffer, the sixth yield blocks inside the feeder.
		for _, err := range chunkflow.New(ctx).Seq2(src).
			MapCtx(func(_ context.Context, i int) (int, error) { return i, nil }, chunkflow.WithParallel(4), chunkflow.WithUnordered()).
			Seq() {
			require.ErrorIs(t, err, boom)
			require.Eventually(t, func() bool { return yielded.Load() == 6 }, 2*time.Second, time.Millisecond)
			break
		}
	})

	t.Run("consumer leaves while every worker is parked on a full output buffer", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		var entered atomic.Int32
		// One result consumed, four in the buffer, four held by workers blocked on the send.
		for _, err := range chunkflow.New(ctx).Seq(seq.Numbers(0)).
			MapCtx(func(_ context.Context, i int) (int, error) { entered.Add(1); return i, nil }, chunkflow.WithParallel(4), chunkflow.WithUnordered()).
			Seq() {
			require.NoError(t, err)
			require.Eventually(t, func() bool { return entered.Load() == 9 }, 2*time.Second, time.Millisecond)
			break
		}
	})

	t.Run("cancelled context surfaces as an error", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		cctx, cancel := context.WithCancel(ctx)
		var started atomic.Int32
		done := make(chan error, 1)
		go func() {
			_, err := chunkflow.New(cctx).Seq(seq.Numbers(0)).
				MapCtx(func(ctx context.Context, i int) (int, error) {
					started.Add(1)
					return blockUntilCancelled(ctx, i)
				}, chunkflow.WithParallel(4), chunkflow.WithUnordered()).
				Collect()
			done <- err
		}()
		require.Eventually(t, func() bool { return started.Load() == 4 }, 2*time.Second, time.Millisecond)
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("pipeline did not terminate after cancel")
		}
	})

	t.Run("cancellation while the consumer is parked drains the buffer, then reports the context error", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var entered atomic.Int32
		gate := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- chunkflow.New(cctx).Seq(seq.Numbers(0)).
				MapCtx(func(_ context.Context, i int) (int, error) { entered.Add(1); return i, nil }, chunkflow.WithParallel(4), chunkflow.WithUnordered()).
				ForEach(func(int) { <-gate }) // the consumer parks on the first element
		}()
		// One result with the consumer, four in the buffer, four held by workers blocked on the send.
		require.Eventually(t, func() bool { return entered.Load() == 9 }, 2*time.Second, time.Millisecond)
		cancel()
		// Let the blocked workers and the feeder observe the cancellation before the consumer
		// makes room again; they leave without emitting, so the pool itself has to report it.
		time.Sleep(20 * time.Millisecond)
		close(gate)

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("pipeline did not terminate after cancel")
		}
		assert.Equal(t, int32(9), entered.Load(), "no item was handed to a worker after cancellation")
	})

	t.Run("has no effect on a single worker", func(t *testing.T) {
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 5)).
			Opts(chunkflow.WithUnordered()).
			MapCtx(func(_ context.Context, i int) (int, error) { return i, nil }).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 2, 3, 4}, res)
	})
}
