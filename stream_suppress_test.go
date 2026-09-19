package chunkflow_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuppress(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")

	t.Run("marks the error and keeps the original in the chain", func(t *testing.T) {
		err := chunkflow.Suppress(boom)
		require.ErrorIs(t, err, chunkflow.ErrSuppressed)
		require.ErrorIs(t, err, boom)
		assert.Equal(t, "suppressed: boom", err.Error())
	})

	t.Run("nil stays nil and a suppressed error is not wrapped twice", func(t *testing.T) {
		require.NoError(t, chunkflow.Suppress(nil))
		once := chunkflow.Suppress(boom)
		assert.Same(t, once, chunkflow.Suppress(once))
	})

	t.Run("refuses context errors and panics", func(t *testing.T) {
		for _, err := range []error{
			context.Canceled,
			fmt.Errorf("fetch: %w", context.DeadlineExceeded),
			fmt.Errorf("wrapped: %w", chunkflow.ErrPanic),
		} {
			got := chunkflow.Suppress(err)
			assert.Same(t, err, got, "returned unchanged")
			assert.NotErrorIs(t, got, chunkflow.ErrSuppressed)
		}
	})

	t.Run("a suppressed callback error drops the element and lets the pipeline continue", func(t *testing.T) {
		var seen []error
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 6)).
			Opts(chunkflow.WithOnError(func(err error) { seen = append(seen, err) })).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i%2 == 1 {
					return -1, chunkflow.Suppress(fmt.Errorf("odd %d: %w", i, boom))
				}
				return i, nil
			}).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 2, 4}, res, "the value returned next to a suppressed error is not emitted")
		require.Len(t, seen, 3)
		for _, e := range seen {
			require.ErrorIs(t, e, chunkflow.ErrSuppressed)
			require.ErrorIs(t, e, boom)
		}
	})

	t.Run("works the same in FilterCtx, TapCtx and on a worker pool", func(t *testing.T) {
		skipOdd := func(i int) error {
			if i%2 == 1 {
				return chunkflow.Suppress(boom)
			}
			return nil
		}
		for name, build := range map[string]func(chunkflow.Stream[int]) chunkflow.Stream[int]{
			"FilterCtx": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
				return s.FilterCtx(func(_ context.Context, i int) (bool, error) { return true, skipOdd(i) })
			},
			"TapCtx": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
				return s.TapCtx(func(_ context.Context, i int) error { return skipOdd(i) })
			},
			"MapCtx parallel": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
				return s.MapCtx(func(_ context.Context, i int) (int, error) { return i, skipOdd(i) }, chunkflow.WithParallel(4))
			},
			"FilterCtx parallel": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
				return s.FilterCtx(func(_ context.Context, i int) (bool, error) { return true, skipOdd(i) }, chunkflow.WithParallel(4))
			},
		} {
			t.Run(name, func(t *testing.T) {
				res, err := build(chunkflow.New(ctx).Seq(seq.Range(0, 10))).Collect()
				require.NoError(t, err)
				assert.Equal(t, []int{0, 2, 4, 6, 8}, res)
			})
		}
	})

	t.Run("Seq exposes suppressed errors in position", func(t *testing.T) {
		var got []string
		for v, err := range chunkflow.New(ctx).Seq(seq.Range(0, 4)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i == 2 {
					return 0, chunkflow.Suppress(boom)
				}
				return i, nil
			}).
			Seq() {
			if err != nil {
				got = append(got, "err")
				continue
			}
			got = append(got, fmt.Sprint(v))
		}
		assert.Equal(t, []string{"0", "1", "err", "3"}, got)
	})

	t.Run("CircuitBreaker does not count errors suppressed by a callback", func(t *testing.T) {
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 10)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				if i < 9 {
					return 0, chunkflow.Suppress(boom) // nine tolerated errors in a row
				}
				return i, nil
			}).
			Through(chunkflow.CircuitBreaker[int](2)).
			Collect()
		require.NoError(t, err, "the breaker would have tripped on the second raw error")
		assert.Equal(t, []int{9}, res)
	})

	t.Run("a suppressed context error from a callback stays fatal", func(t *testing.T) {
		_, err := chunkflow.New(ctx).Seq(seq.Range(0, 3)).
			MapCtx(func(_ context.Context, i int) (int, error) {
				return i, chunkflow.Suppress(fmt.Errorf("client: %w", context.Canceled))
			}).
			Collect()
		require.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, chunkflow.ErrSuppressed)
	})
}
