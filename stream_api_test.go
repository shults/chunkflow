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
)

func TestStream_AlignedWithStream(t *testing.T) {
	ctx := t.Context()

	t.Run("Filter drops non-matching items", func(t *testing.T) {
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(1, 10)).
			Filter(func(i int) bool { return i%2 == 0 }).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{2, 4, 6, 8}, res)
	})

	t.Run("Tap observes values and passes them through", func(t *testing.T) {
		var seen []int
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(1, 4)).
			Tap(func(i int) { seen = append(seen, i) }).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, res)
		assert.Equal(t, []int{1, 2, 3}, seen)
	})

	t.Run("Tap never sees errors and lets them flow", func(t *testing.T) {
		var seen []int
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(failing). // 2,3,4 fail
			Tap(func(i int) { seen = append(seen, i) }).
			CircuitBreaker(100).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, res)
		assert.Equal(t, []int{0, 1, 5, 6, 7, 8, 9}, seen)
	})

	t.Run("TapCtx error replaces the value and is fatal for terminals", func(t *testing.T) {
		errAudit := errors.New("audit")
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(1, 6)).
			TapCtx(func(_ context.Context, i int) error {
				if i == 3 {
					return errAudit
				}
				return nil
			}).
			Collect()
		require.ErrorIs(t, err, errAudit)
		assert.Equal(t, []int{1, 2}, res)
	})

	t.Run("TapCtx receives the stream context and runs in parallel", func(t *testing.T) {
		type key struct{}
		cctx := context.WithValue(ctx, key{}, "tap")
		var okCtx, calls atomic.Int32

		res, err := chunkflow.
			New(cctx).Seq(seq.Range(0, 20)).
			TapCtx(func(ctx context.Context, _ int) error {
				calls.Add(1)
				if v, _ := ctx.Value(key{}).(string); v == "tap" {
					okCtx.Add(1)
				}
				return nil
			}, chunkflow.WithParallel(4)).
			Collect()
		require.NoError(t, err)
		assert.Len(t, res, 20)
		assert.Equal(t, int32(20), calls.Load())
		assert.Equal(t, int32(20), okCtx.Load())
	})

	t.Run("Any short-circuits on first match", func(t *testing.T) {
		evaluated := 0
		ok, err := chunkflow.
			New(ctx).Seq(seq.Numbers(1)).
			Map(func(i int) int {
				evaluated++
				return i
			}).
			Any(func(i int) bool { return i == 3 })
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 3, evaluated)
	})

	t.Run("First returns element or reports empty", func(t *testing.T) {
		val, ok, err := chunkflow.New(ctx).Seq(seq.Items(99, 100)).First()
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 99, val)

		val, ok, err = chunkflow.New(ctx).Seq(seq.Items[int]()).First()
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, 0, val)
	})

	t.Run("First propagates error", func(t *testing.T) {
		boom := errors.New("boom")
		_, ok, err := chunkflow.
			New(ctx).Seq(seq.Items(1)).
			MapCtx(func(context.Context, int) (int, error) { return 0, boom }).
			First()
		require.ErrorIs(t, err, boom)
		assert.False(t, ok)
	})

	t.Run("Last returns final element", func(t *testing.T) {
		val, ok, err := chunkflow.New(ctx).Seq(seq.Items(1, 2, 99)).Last()
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 99, val)
	})

	t.Run("Seq exposes iter.Seq2 and stops after first error", func(t *testing.T) {
		boom := errors.New("boom")
		var vals []int
		var errs []error
		s := chunkflow.
			New(ctx).Seq(seq.Items(1, 2, 3)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i == 2 {
					return 0, boom
				}
				return i, nil
			})
		for v, err := range s.Seq() {
			vals = append(vals, v)
			errs = append(errs, err)
		}
		assert.Equal(t, []int{1, 0}, vals)
		assert.Equal(t, []error{nil, boom}, errs)
	})

	t.Run("Seq2 round-trips through Seq", func(t *testing.T) {
		boom := errors.New("boom")
		src := chunkflow.
			New(ctx).Seq(seq.Items(1, 2, 3)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i == 3 {
					return 0, boom
				}
				return i * 10, nil
			})

		res, err := chunkflow.New(ctx).Seq2(src.Seq()).Collect()
		require.ErrorIs(t, err, boom)
		assert.Equal(t, []int{10, 20}, res)
	})

	t.Run("Flatten via Through", func(t *testing.T) {
		res, err := chunkflow.
			New(ctx).Seq(seq.Range(1, 6)).
			Chunk[[]int](2).
			Through(chunkflow.Flatten).
			Map(func(i int) int { return i * 10 }).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{10, 20, 30, 40, 50}, res)
	})

	t.Run("Count returns number of elements or count before error", func(t *testing.T) {
		n, err := chunkflow.New(ctx).Seq(seq.Range(0, 7)).Count()
		require.NoError(t, err)
		assert.Equal(t, 7, n)

		boom := errors.New("boom")
		n, err = chunkflow.
			New(ctx).Seq(seq.Items(1, 2, 3)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i == 3 {
					return 0, boom
				}
				return i, nil
			}).
			Count()
		require.ErrorIs(t, err, boom)
		assert.Equal(t, 2, n)
	})

	t.Run("ForEachCtx stops on callback error", func(t *testing.T) {
		boom := errors.New("boom")
		var seen []int
		err := chunkflow.
			New(ctx).Seq(seq.Items(1, 2, 3)).
			ForEachCtx(func(_ context.Context, i int) error {
				seen = append(seen, i)
				if i == 2 {
					return boom
				}
				return nil
			})
		require.ErrorIs(t, err, boom)
		assert.Equal(t, []int{1, 2}, seen)
	})
}

func TestStream_Options(t *testing.T) {
	ctx := t.Context()

	t.Run("Opts are inherited by derived streams", func(t *testing.T) {
		const workers = 4
		var entered atomic.Int32
		var timedOut atomic.Bool
		gate := make(chan struct{})

		// Release all workers once every one of them is blocked inside mapFn,
		// or give up after a timeout so a regression fails instead of hanging.
		go func() {
			defer close(gate)
			deadline := time.After(2 * time.Second)
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for entered.Load() < workers {
				select {
				case <-deadline:
					timedOut.Store(true)
					return
				case <-tick.C:
				}
			}
		}()

		res, err := chunkflow.
			New(ctx).Seq(seq.Range(0, workers)).
			Opts(chunkflow.WithParallel(workers)).
			Skip(0). // derived stream must still carry the configured concurrency
			MapCtx(func(_ context.Context, i int) (int, error) {
				entered.Add(1)
				<-gate
				return i, nil
			}).
			Collect()

		require.NoError(t, err)
		assert.ElementsMatch(t, []int{0, 1, 2, 3}, res)
		assert.False(t, timedOut.Load(), "not all workers ran concurrently; options were not inherited")
	})

	t.Run("WithOnError sees suppressed errors and the fatal one, in order", func(t *testing.T) {
		var seen []error
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 10)).
			Opts(chunkflow.WithOnError(func(err error) { seen = append(seen, err) })).
			MapCtx(failing). // 2,3,4 fail
			CircuitBreaker(3).
			Collect()

		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, []int{0, 1}, res)
		require.Len(t, seen, 3, "two suppressed + the tripping error")
		require.ErrorIs(t, seen[0], chunkflow.ErrSuppressed)
		require.ErrorIs(t, seen[1], chunkflow.ErrSuppressed)
		require.NotErrorIs(t, seen[2], chunkflow.ErrSuppressed)
		assert.Same(t, err, seen[2], "the last reported error is the one returned")
	})

	t.Run("WithOnError sees terminal callback errors too", func(t *testing.T) {
		errCb := errors.New("callback")
		var seen []error
		err := chunkflow.New(ctx, chunkflow.WithOnError(func(err error) { seen = append(seen, err) })).
			Seq(seq.Items(1, 2)).
			ForEachCtx(func(context.Context, int) error { return errCb })
		require.ErrorIs(t, err, errCb)
		assert.Equal(t, []error{errCb}, seen)
	})

	t.Run("WithOnError is inherited through derived streams", func(t *testing.T) {
		calls := 0
		_, err := chunkflow.New(ctx, chunkflow.WithOnError(func(error) { calls++ })).Seq(seq.Range(0, 5)).
			MapCtx(failing). // 2,3,4 fail
			CircuitBreaker(100).
			Skip(0).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, 3, calls)
	})

	t.Run("invalid options become an error stream wherever they are applied", func(t *testing.T) {
		src := func() chunkflow.Stream[int] { return chunkflow.New(ctx).Seq(seq.Items(1, 2, 3)) }

		_, err := src().Opts(chunkflow.WithOnError(nil)).Collect()
		require.ErrorContains(t, err, "WithOnError: nil callback")

		_, err = chunkflow.New(ctx, chunkflow.WithOnError(nil)).Seq(seq.Items(1)).Collect()
		require.ErrorContains(t, err, "WithOnError: nil callback")

		_, err = chunkflow.New(ctx, chunkflow.WithParallel(0)).Chan(make(chan int)).Collect()
		require.ErrorContains(t, err, "concurrency must be at least 1", "reported even though the channel would block")

		_, err = src().Opts(chunkflow.WithParallel(-1)).Collect()
		require.ErrorContains(t, err, "WithParallel(-1)")

		res, err := src().Opts(chunkflow.WithParallel(2)).Collect()
		require.NoError(t, err)
		assert.ElementsMatch(t, []int{1, 2, 3}, res)
	})

	t.Run("WithParallel is both a step and a pipeline option", func(t *testing.T) {
		step := chunkflow.WithParallel(2)
		var pipeline chunkflow.Option = step // compiles: every StepOption is an Option
		assert.NotNil(t, pipeline)
	})

	t.Run("FilterCtx rejects concurrency below 1", func(t *testing.T) {
		_, err := chunkflow.
			New(ctx).Seq(seq.Items(1)).
			FilterCtx(func(context.Context, int) (bool, error) { return true, nil }, chunkflow.WithParallel(0)).
			Collect()
		assert.ErrorContains(t, err, "concurrency must be at least 1")
	})

	t.Run("cancelled context surfaces as error", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := chunkflow.New(cctx).Seq(seq.Numbers(0)).Take(3).Collect()
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("cancelled context surfaces as error through parallel stages", func(t *testing.T) {
		// Workers and the feeder may all pick the ctx.Done() branch and exit without
		// emitting anything; the stage must still report the cancellation.
		for range 50 {
			cctx, cancel := context.WithCancel(ctx)
			cancel()

			_, err := chunkflow.
				New(cctx).Seq(seq.Range(0, 100)).
				MapCtx(func(_ context.Context, i int) (int, error) { return i, nil }, chunkflow.WithParallel(4)).
				Collect()
			require.ErrorIs(t, err, context.Canceled)

			_, err = chunkflow.
				New(cctx).Seq(seq.Range(0, 100)).
				FilterCtx(func(context.Context, int) (bool, error) { return true, nil }, chunkflow.WithParallel(4)).
				Collect()
			require.ErrorIs(t, err, context.Canceled)
		}
	})
}
