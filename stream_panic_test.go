package chunkflow_test

import (
	"context"
	"errors"
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

var errInside = errors.New("inside")

// explode panics on 3 with a plain string and on 5 with an error value.
func explode(_ context.Context, i int) (int, error) {
	switch i {
	case 3:
		panic("boom at 3")
	case 5:
		panic(errInside)
	}
	return i, nil
}

func TestStream_PanicsBecomeErrors(t *testing.T) {
	ctx := t.Context()

	t.Run("sequential MapCtx: panic is returned as ErrPanic with partial results", func(t *testing.T) {
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 10)).MapCtx(explode).Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
		require.ErrorContains(t, err, "boom at 3")
		assert.Contains(t, err.Error(), "stream_panic_test.go", "the stack of the panicking goroutine is in the message")
		assert.Equal(t, []int{0, 1, 2}, res)
	})

	t.Run("parallel MapCtx: the worker panic does not kill the process and releases the pool", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		_, err := chunkflow.New(ctx).Seq(seq.Numbers(0)). // infinite: workers must be released
									MapCtx(explode, chunkflow.WithParallel(4)).
									Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
	})

	t.Run("one worker panics while the others are busy: they are all shut down", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		const workers = 4
		var blocked, released atomic.Int32
		panicked := make(chan struct{})

		_, err := chunkflow.
			New(ctx).
			Seq(seq.Numbers(0)). // infinite source
			MapCtx(func(ctx context.Context, i int) (int, error) {
				if i == 0 {
					// let the other workers park in their callbacks before blowing up
					require.Eventually(t, func() bool { return blocked.Load() == workers-1 }, 2*time.Second, time.Millisecond)
					close(panicked)
					panic("worker 0 is done for")
				}
				blocked.Add(1)
				<-ctx.Done() // only the pool's cancellation can release this worker
				released.Add(1)
				return i, nil
			}, chunkflow.WithParallel(workers)).
			Collect()

		require.ErrorIs(t, err, chunkflow.ErrPanic)
		<-panicked
		// At least the three parked workers must have been released. The panicking worker may
		// grab one more buffered item before it notices the cancellation and be released too,
		// hence >=. goleak (deferred above) proves nothing is left hanging.
		require.Eventually(t, func() bool { return released.Load() >= workers-1 }, 2*time.Second, time.Millisecond,
			"the remaining workers must observe the cancellation and exit")
	})

	t.Run("a panic with an error value keeps that error in the chain", func(t *testing.T) {
		_, err := chunkflow.New(ctx).Seq(seq.Items(5)).MapCtx(explode).Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
		require.ErrorIs(t, err, errInside)
	})

	t.Run("CircuitBreaker never suppresses a panic and ends the stream", func(t *testing.T) {
		var reported []error
		res, err := chunkflow.
			New(ctx).
			Seq(seq.Range(0, 10)).
			Opts(chunkflow.WithOnError(func(err error) { reported = append(reported, err) })).
			MapCtx(func(ctx context.Context, i int) (int, error) {
				if i == 1 {
					return 0, errBoom // a normal, tolerable error
				}
				return explode(ctx, i)
			}).
			Through(policy.CircuitBreaker[int](100)).
			Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
		assert.Equal(t, []int{0, 2}, res, "1 was suppressed, 3 panicked and ended the stream")
		require.Len(t, reported, 2)
		require.ErrorIs(t, reported[0], chunkflow.ErrSuppressed)
		assert.NotErrorIs(t, reported[1], chunkflow.ErrSuppressed)
	})

	t.Run("panics in predicates and terminal callbacks are converted too", func(t *testing.T) {
		boom := func(context.Context, int) (bool, error) { panic("pred") }

		_, err := chunkflow.New(ctx).Seq(seq.Items(1)).FilterCtx(boom).Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
		_, err = chunkflow.New(ctx).Seq(seq.Items(1)).FilterCtx(boom, chunkflow.WithParallel(2)).Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
		_, err = chunkflow.New(ctx).Seq(seq.Items(1)).TakeWhileCtx(boom).Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)
		_, err = chunkflow.New(ctx).Seq(seq.Items(1)).SkipWhileCtx(boom).Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)

		_, err = chunkflow.New(ctx).Seq(seq.Items(1, 2)).CompactFunc(func(int, int) bool { panic("eq") }).Collect()
		require.ErrorIs(t, err, chunkflow.ErrPanic)

		err = chunkflow.New(ctx).Seq(seq.Items(1)).ForEach(func(int) { panic("terminal") })
		require.ErrorIs(t, err, chunkflow.ErrPanic)
		assert.ErrorContains(t, err, "terminal")
	})

	t.Run("CompactFunc: a panicking eq is reported in the element's position", func(t *testing.T) {
		var values []int
		var errs []error
		for v, err := range chunkflow.New(ctx).Seq(seq.Items(1, 1, 2)).
			CompactFunc(func(int, int) bool { panic("eq") }).Seq() {
			if err != nil {
				errs = append(errs, err)
				continue
			}
			values = append(values, v)
		}
		assert.Equal(t, []int{1}, values, "the first element needs no comparison and is emitted")
		require.Len(t, errs, 1, "Seq stops at the panic like at any other fatal error")
		require.ErrorIs(t, errs[0], chunkflow.ErrPanic)
	})

	t.Run("Seq exposes the panic as an element in position", func(t *testing.T) {
		var values []int
		var errs []error
		for v, err := range chunkflow.New(ctx).Seq(seq.Range(0, 10)).MapCtx(explode).Seq() {
			if err != nil {
				errs = append(errs, err)
				continue
			}
			values = append(values, v)
		}
		assert.Equal(t, []int{0, 1, 2}, values)
		require.Len(t, errs, 1)
		assert.ErrorIs(t, errs[0], chunkflow.ErrPanic)
	})

	t.Run("a panic in the consumer is not mistaken for a callback panic", func(t *testing.T) {
		// The guard is deferred once per stage; a panic raised by the loop body inside yield
		// must pass through untouched instead of being reported as ErrPanic.
		assert.PanicsWithValue(t, "consumer", func() {
			for range chunkflow.New(ctx).Seq(seq.Items(1)).MapCtx(func(_ context.Context, i int) (int, error) { return i, nil }).Seq() {
				panic("consumer")
			}
		})
	})
}
