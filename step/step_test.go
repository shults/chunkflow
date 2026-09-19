package step_test

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
	"github.com/shults/chunkflow/step"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var (
	errBoom     = errors.New("boom")
	errNotFound = errors.New("not found")
)

// failN fails the first n calls and counts every call.
func failN(n int, calls *atomic.Int32) func(context.Context, int) (int, error) {
	return func(_ context.Context, i int) (int, error) {
		if calls.Add(1) <= int32(n) {
			return 0, errBoom
		}
		return i * 10, nil
	}
}

// backoffFunc adapts a function to step.Backoff for tests.
type backoffFunc func() time.Duration

func (f backoffFunc) NextBackOff() time.Duration { return f() }
func (backoffFunc) Reset()                       {}

// thirdParty has exactly the method set of github.com/cenkalti/backoff/v4.BackOff, so the
// assignment below is the compile-time proof that such strategies plug in unchanged.
type thirdParty struct{ resets, calls int }

func (b *thirdParty) NextBackOff() time.Duration { b.calls++; return 0 }
func (b *thirdParty) Reset()                     { b.resets++ }

var _ step.Backoff = (*thirdParty)(nil)

// trace is a middleware that records when it is entered and left, to observe ordering.
func trace(name string, log *[]string) step.Middleware {
	return func(ctx context.Context, call func(context.Context) error) error {
		*log = append(*log, "+"+name)
		err := call(ctx)
		*log = append(*log, "-"+name)
		return err
	}
}

func TestDecorate(t *testing.T) {
	ctx := t.Context()

	t.Run("without middleware it is the plain callback", func(t *testing.T) {
		res, err := step.Decorate(func(_ context.Context, i int) (int, error) { return i * 2, nil })(ctx, 21)
		require.NoError(t, err)
		assert.Equal(t, 42, res)
	})

	t.Run("the first middleware is the outermost", func(t *testing.T) {
		var log []string
		fn := step.Decorate(func(_ context.Context, i int) (int, error) {
			log = append(log, "fn")
			return i, nil
		}, trace("a", &log), trace("b", &log))
		_, err := fn(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, []string{"+a", "+b", "fn", "-b", "-a"}, log)
	})

	t.Run("a suppressed error from the chain is returned marked, with a zero result", func(t *testing.T) {
		fn := step.Decorate(func(_ context.Context, i int) (int, error) { return 99, errNotFound }, step.Tolerate(errNotFound))
		res, err := fn(ctx, 1)
		require.ErrorIs(t, err, chunkflow.ErrSuppressed)
		require.ErrorIs(t, err, errNotFound)
		assert.Equal(t, 0, res, "the value returned next to the error is dropped")
	})

	t.Run("a plain error is returned with a zero result", func(t *testing.T) {
		fn := step.Decorate(func(_ context.Context, i int) (int, error) { return 99, errBoom })
		res, err := fn(ctx, 1)
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, 0, res)
	})
}

func TestDecorate3(t *testing.T) {
	ctx := t.Context()
	add := func(_ context.Context, acc, i int) (int, error) { return acc + i, nil }

	t.Run("without middleware it is the plain fold callback", func(t *testing.T) {
		acc, err := step.Decorate3(add)(ctx, 10, 5)
		require.NoError(t, err)
		assert.Equal(t, 15, acc)
	})

	t.Run("every retry starts from the same accumulator", func(t *testing.T) {
		var calls atomic.Int32
		var seenAcc []int
		flaky := func(_ context.Context, acc, i int) (int, error) {
			seenAcc = append(seenAcc, acc)
			if calls.Add(1) < 3 {
				return acc + 1000, errBoom // a failed attempt "returns" garbage; it must be discarded
			}
			return acc + i, nil
		}
		acc, err := step.Decorate3(flaky, step.Retry(3))(ctx, 10, 5)
		require.NoError(t, err)
		assert.Equal(t, 15, acc)
		assert.Equal(t, []int{10, 10, 10}, seenAcc)
	})

	t.Run("a suppressed error skips the element: same accumulator, no error", func(t *testing.T) {
		lookup := func(_ context.Context, acc, i int) (int, error) {
			if i == 2 {
				return acc + 1000, errNotFound
			}
			return acc + i, nil
		}
		fn := step.Decorate3(lookup, step.Tolerate(errNotFound))
		acc, err := fn(ctx, 10, 2)
		require.NoError(t, err)
		assert.Equal(t, 10, acc)
		acc, err = fn(ctx, 10, 3)
		require.NoError(t, err)
		assert.Equal(t, 13, acc)
	})

	t.Run("a plain error keeps the accumulator and is returned", func(t *testing.T) {
		boom := func(_ context.Context, acc, i int) (int, error) { return acc + 1000, errBoom }
		acc, err := step.Decorate3(boom)(ctx, 10, 1)
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, 10, acc)
	})

	t.Run("in a real fold the tolerated element is skipped and the fold continues", func(t *testing.T) {
		var calls atomic.Int32
		insert := func(_ context.Context, n, i int) (int, error) {
			switch {
			case i == 2:
				return n, errNotFound // skipped
			case i == 4 && calls.Add(1) == 1:
				return n, errBoom // fails once, retried
			}
			return n + 1, nil
		}
		n, err := chunkflow.New(ctx).Seq(seq.Range(0, 6)).
			ReduceCtx(0, step.Decorate3(insert, step.Retry(2), step.Tolerate(errNotFound)))
		require.NoError(t, err)
		assert.Equal(t, 5, n, "six elements, one skipped")
	})
}

func TestRetry(t *testing.T) {
	ctx := t.Context()

	t.Run("returns the first success and stops calling", func(t *testing.T) {
		var calls atomic.Int32
		res, err := step.Decorate(failN(2, &calls), step.Retry(3))(ctx, 4)
		require.NoError(t, err)
		assert.Equal(t, 40, res)
		assert.Equal(t, int32(3), calls.Load())
	})

	t.Run("returns the last error unchanged after the last attempt", func(t *testing.T) {
		var calls atomic.Int32
		_, err := step.Decorate(failN(10, &calls), step.Retry(3))(ctx, 4)
		assert.Same(t, errBoom, err, "no wrapping: the decorator is transparent on failure")
		assert.Equal(t, int32(3), calls.Load())
	})

	t.Run("does not retry an error the callback already suppressed", func(t *testing.T) {
		var calls atomic.Int32
		fn := func(context.Context, int) (int, error) { calls.Add(1); return 0, chunkflow.Suppress(errBoom) }
		_, err := step.Decorate(fn, step.Retry(5))(ctx, 1)
		require.ErrorIs(t, err, chunkflow.ErrSuppressed)
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("does not retry what an inner Tolerate decided to skip", func(t *testing.T) {
		var calls atomic.Int32
		fn := func(context.Context, int) (int, error) { calls.Add(1); return 0, errNotFound }
		_, err := step.Decorate(fn, step.Retry(5), step.Tolerate(errNotFound))(ctx, 1)
		require.ErrorIs(t, err, chunkflow.ErrSuppressed)
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("does not retry once the caller's context is done", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		var calls atomic.Int32
		fn := func(context.Context, int) (int, error) {
			calls.Add(1)
			cancel() // the pipeline is told to stop while we fail
			return 0, errBoom
		}
		_, err := step.Decorate(fn, step.Retry(5))(cctx, 1)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, errBoom, "the failure that was not retried stays in the chain")
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("retries an attempt that hit an inner Timeout", func(t *testing.T) {
		var calls atomic.Int32
		slowThenFast := func(ctx context.Context, i int) (int, error) {
			if calls.Add(1) == 1 {
				<-ctx.Done() // the first attempt hangs until its own deadline
				return 0, ctx.Err()
			}
			return i, nil
		}
		res, err := step.Decorate(slowThenFast, step.Retry(2), step.Timeout(10*time.Millisecond))(ctx, 7)
		require.NoError(t, err)
		assert.Equal(t, 7, res)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("a Backoff paces the attempts and may end the series early", func(t *testing.T) {
		var calls atomic.Int32
		var asked int
		pacing := func() step.Backoff {
			return backoffFunc(func() time.Duration { asked++; return 20 * time.Millisecond })
		}
		start := time.Now()
		res, err := step.Decorate(failN(2, &calls), step.Retry(3, step.RetryWithBackoff(pacing)))(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, 10, res)
		assert.Equal(t, 2, asked, "asked after each failed attempt that may be repeated, never after a success")
		assert.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)

		calls.Store(0)
		asked = 0
		_, err = step.Decorate(failN(10, &calls), step.Retry(3, step.RetryWithBackoff(pacing)))(ctx, 1)
		assert.Same(t, errBoom, err)
		assert.Equal(t, 2, asked, "not asked after the last allowed attempt")
		assert.Equal(t, int32(3), calls.Load())

		calls.Store(0)
		giveUp := func() step.Backoff { return backoffFunc(func() time.Duration { return step.Stop }) }
		_, err = step.Decorate(failN(10, &calls), step.Retry(10, step.RetryWithBackoff(giveUp)))(ctx, 1)
		assert.Same(t, errBoom, err)
		assert.Equal(t, int32(1), calls.Load(), "the Backoff ended the series before the attempt limit")
	})

	t.Run("a third-party constructor plugs in and is reset once per series", func(t *testing.T) {
		var instances []*thirdParty
		newBackoff := func() step.Backoff { // a concrete constructor, as cenkalti's, is wrapped like this
			b := &thirdParty{}
			instances = append(instances, b)
			return b
		}
		var calls atomic.Int32
		_, err := step.Decorate(failN(10, &calls), step.Retry(3, step.RetryWithBackoff(newBackoff)))(ctx, 1)
		assert.Same(t, errBoom, err)
		require.Len(t, instances, 1)
		assert.Equal(t, 1, instances[0].resets, "reset before the first attempt")
		assert.Equal(t, 2, instances[0].calls, "asked after the two failures that could be repeated")
	})

	t.Run("every series gets a fresh Backoff, also on a worker pool", func(t *testing.T) {
		var created atomic.Int32
		newBackoff := func() step.Backoff {
			created.Add(1)
			return backoffFunc(func() time.Duration { return 0 })
		}
		var failedOnce [8]atomic.Bool
		flaky := func(_ context.Context, i int) (int, error) {
			if !failedOnce[i].Swap(true) {
				return 0, errBoom // every element fails exactly once, then succeeds
			}
			return i, nil
		}
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 8)).
			MapCtx(step.Decorate(flaky, step.Retry(3, step.RetryWithBackoff(newBackoff))), chunkflow.WithParallel(4)).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, res)
		assert.Equal(t, int32(8), created.Load(), "one Backoff per element, none shared")
	})

	t.Run("a pause is cut short by cancellation", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		var calls atomic.Int32
		go func() { time.Sleep(10 * time.Millisecond); cancel() }()
		start := time.Now()
		hour := func() step.Backoff { return backoffFunc(func() time.Duration { return time.Hour }) }
		_, err := step.Decorate(failN(10, &calls), step.Retry(3, step.RetryWithBackoff(hour)))(cctx, 1)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, errBoom)
		assert.Less(t, time.Since(start), time.Second)
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("a panic is not retried, it propagates to the stage guard", func(t *testing.T) {
		var calls atomic.Int32
		exploding := func(context.Context, int) (int, error) { calls.Add(1); panic("bug") }
		assert.PanicsWithValue(t, "bug", func() { _, _ = step.Decorate(exploding, step.Retry(3))(ctx, 1) })
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("misconfiguration fails every call without running the callback", func(t *testing.T) {
		var calls atomic.Int32
		_, err := step.Decorate(failN(0, &calls), step.Retry(0))(ctx, 1)
		require.ErrorContains(t, err, "Retry(0): attempts must be at least 1")
		_, err = step.Decorate(failN(0, &calls), step.Retry(3, step.RetryWithBackoff(nil)))(ctx, 1)
		require.ErrorContains(t, err, "RetryWithBackoff: nil constructor")
		assert.Equal(t, int32(0), calls.Load())
	})
}

func TestTimeout(t *testing.T) {
	ctx := t.Context()
	hang := func(ctx context.Context, i int) (int, error) { <-ctx.Done(); return 0, ctx.Err() }

	t.Run("bounds a cooperative call", func(t *testing.T) {
		start := time.Now()
		_, err := step.Decorate(hang, step.Timeout(10*time.Millisecond))(ctx, 1)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Less(t, time.Since(start), time.Second)
	})

	t.Run("a fast call is untouched and sees a deadline", func(t *testing.T) {
		res, err := step.Decorate(func(ctx context.Context, i int) (int, error) {
			_, has := ctx.Deadline()
			assert.True(t, has)
			return i + 1, nil
		}, step.Timeout(time.Second))(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, 2, res)
	})

	t.Run("outside Retry it bounds all attempts together", func(t *testing.T) {
		var calls atomic.Int32
		counting := func(ctx context.Context, i int) (int, error) { calls.Add(1); return hang(ctx, i) }
		_, err := step.Decorate(counting, step.Timeout(20*time.Millisecond), step.Retry(5))(ctx, 1)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, int32(1), calls.Load(), "the budget is gone and the context Retry sees is done: no retry")
	})

	t.Run("non-positive duration fails every call", func(t *testing.T) {
		_, err := step.Decorate(hang, step.Timeout(0))(ctx, 1)
		require.ErrorContains(t, err, "Timeout(0s): duration must be positive")
	})
}

func TestTolerate(t *testing.T) {
	ctx := t.Context()
	lookup := func(_ context.Context, i int) (int, error) {
		switch i {
		case 2:
			return 0, errNotFound
		case 3:
			return 0, errBoom
		}
		return i, nil
	}

	t.Run("suppresses the target and passes everything else", func(t *testing.T) {
		fn := step.Decorate(lookup, step.Tolerate(errNotFound))

		res, err := fn(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, 1, res)

		_, err = fn(ctx, 2)
		require.ErrorIs(t, err, chunkflow.ErrSuppressed)
		require.ErrorIs(t, err, errNotFound)

		_, err = fn(ctx, 3)
		require.ErrorIs(t, err, errBoom)
		assert.NotErrorIs(t, err, chunkflow.ErrSuppressed)
	})

	t.Run("several targets in one call, or by stacking", func(t *testing.T) {
		oneCall := step.Decorate(lookup, step.Tolerate(errNotFound, errBoom))
		stacked := step.Decorate(lookup, step.Tolerate(errNotFound), step.Tolerate(errBoom))
		for _, fn := range []func(context.Context, int) (int, error){oneCall, stacked} {
			for _, i := range []int{2, 3} {
				_, err := fn(ctx, i)
				require.ErrorIs(t, err, chunkflow.ErrSuppressed)
			}
			res, err := fn(ctx, 1)
			require.NoError(t, err)
			assert.Equal(t, 1, res)
		}
	})

	t.Run("wrapped errors match through the chain", func(t *testing.T) {
		wrapped := func(context.Context, int) (int, error) { return 0, fmt.Errorf("lookup 7: %w", errNotFound) }
		_, err := step.Decorate(wrapped, step.Tolerate(errBoom, errNotFound))(ctx, 7)
		require.ErrorIs(t, err, chunkflow.ErrSuppressed)
		require.ErrorIs(t, err, errNotFound)
	})

	t.Run("context errors as target are a no-op, Suppress refuses them", func(t *testing.T) {
		cancelled := func(context.Context, int) (int, error) { return 0, context.Canceled }
		_, err := step.Decorate(cancelled, step.Tolerate(context.Canceled))(ctx, 1)
		require.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, chunkflow.ErrSuppressed)
	})

	t.Run("no targets or a nil target fails every call", func(t *testing.T) {
		_, err := step.Decorate(lookup, step.Tolerate())(ctx, 1)
		require.ErrorContains(t, err, "Tolerate: no targets")
		_, err = step.Decorate(lookup, step.Tolerate(errNotFound, nil))(ctx, 1)
		require.ErrorContains(t, err, "Tolerate: nil target at index 1")
	})
}

func TestOneMiddlewareSetServesBothShapes(t *testing.T) {
	ctx := t.Context()
	mws := []step.Middleware{step.Retry(3), step.Tolerate(errNotFound)}

	var calls atomic.Int32
	// The same fallible I/O, once as a map callback and once inside a fold callback.
	fetch := func(_ context.Context, i int) (int, error) {
		switch {
		case i == 2:
			return 0, errNotFound
		case i == 4 && calls.Add(1) <= 2:
			return 0, errBoom
		}
		return i * 10, nil
	}
	sumFetched := func(ctx context.Context, acc, i int) (int, error) {
		v, err := fetch(ctx, i)
		return acc + v, err
	}

	mapped, err := chunkflow.New(ctx).Seq(seq.Range(0, 6)).
		MapCtx(step.Decorate(fetch, mws...), chunkflow.WithParallel(3)).
		Through(policy.CircuitBreaker[int](2)).
		Collect()
	require.NoError(t, err)
	assert.Equal(t, []int{0, 10, 30, 40, 50}, mapped, "2 skipped, 4 recovered, order kept")

	calls.Store(0)
	sum, err := chunkflow.New(ctx).Seq(seq.Range(0, 6)).
		ReduceCtx(0, step.Decorate3(sumFetched, mws...))
	require.NoError(t, err)
	assert.Equal(t, 0+10+30+40+50, sum, "the same middlewares, the same outcome, in a fold")
}
