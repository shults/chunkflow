package chunkflow_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errAfter yields 0..n-1 successfully and then a single error, keeping the source alive.
func errAfter(n int, err error) func(func(int, error) bool) {
	return func(yield func(int, error) bool) {
		for i := range n {
			if !yield(i, nil) {
				return
			}
		}
		yield(0, err)
	}
}

func TestIoStream_Exec(t *testing.T) {
	ctx := t.Context()

	t.Run("exhausts the stream and triggers side effects", func(t *testing.T) {
		var seen int
		err := chunkflow.
			NewIo(ctx).Seq(seq.Range(0, 5)).
			Map(func(i int) int { seen++; return i }).
			Exec()
		require.NoError(t, err)
		assert.Equal(t, 5, seen)
	})

	t.Run("is a no-op on an empty stream", func(t *testing.T) {
		require.NoError(t, chunkflow.NewIo(ctx).Seq(seq.Items[int]()).Exec())
	})

	t.Run("returns the first upstream error and stops consuming", func(t *testing.T) {
		var pulled int
		err := chunkflow.
			NewIo(ctx).Seq2(errAfter(3, errBoom)).
			Map(func(i int) int { pulled++; return i }).
			Exec()
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, 3, pulled)
	})

	t.Run("returns a cancelled context as error", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, chunkflow.NewIo(cctx).Seq(seq.Numbers(0)).Exec(), context.Canceled)
	})
}

func TestIoStream_Reduce(t *testing.T) {
	ctx := t.Context()
	sum := func(item, acc int) int { return acc + item }

	t.Run("aggregates all values starting from init", func(t *testing.T) {
		total, err := chunkflow.NewIo(ctx).Seq(seq.Range(1, 5)).Reduce(100, sum)
		require.NoError(t, err)
		assert.Equal(t, 110, total)
	})

	t.Run("returns init for an empty stream", func(t *testing.T) {
		total, err := chunkflow.NewIo(ctx).Seq(seq.Items[int]()).Reduce(42, sum)
		require.NoError(t, err)
		assert.Equal(t, 42, total)
	})

	t.Run("passes item first and accumulator second", func(t *testing.T) {
		var order [][2]int
		_, err := chunkflow.NewIo(ctx).Seq(seq.Items(10, 20)).Reduce(1, func(item, acc int) int {
			order = append(order, [2]int{item, acc})
			return acc + item
		})
		require.NoError(t, err)
		assert.Equal(t, [][2]int{{10, 1}, {20, 11}}, order)
	})

	t.Run("returns the partial accumulator together with an upstream error", func(t *testing.T) {
		total, err := chunkflow.NewIo(ctx).Seq2(errAfter(3, errBoom)).Reduce(0, sum) // 0+1+2
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, 3, total)
	})

	t.Run("skips suppressed errors", func(t *testing.T) {
		total, err := tolerant(ctx).Reduce(0, sum) // 0,1,5,6,7,8,9
		require.NoError(t, err)
		assert.Equal(t, 36, total)
	})
}

func TestIoStream_ReduceCtx(t *testing.T) {
	ctx := t.Context()

	t.Run("receives the stream context", func(t *testing.T) {
		type key struct{}
		cctx := context.WithValue(ctx, key{}, 42)

		got, err := chunkflow.NewIo(cctx).Seq(seq.Items(1)).
			ReduceCtx(0, func(ctx context.Context, _, _ int) (int, error) {
				v, _ := ctx.Value(key{}).(int)
				return v, nil
			})
		require.NoError(t, err)
		assert.Equal(t, 42, got)
	})

	t.Run("callback error is returned with the accumulator so far", func(t *testing.T) {
		errCb := errors.New("callback")
		total, err := chunkflow.NewIo(ctx).Seq(seq.Range(1, 10)).
			ReduceCtx(0, func(_ context.Context, item, acc int) (int, error) {
				if item == 4 {
					return acc, errCb
				}
				return acc + item, nil
			})
		require.ErrorIs(t, err, errCb)
		assert.Equal(t, 6, total, "1+2+3 accumulated before the failure")
	})

	t.Run("concurrency above 1 warns and reduces sequentially", func(t *testing.T) {
		var buf bytes.Buffer
		var order []int
		total, err := chunkflow.NewIo(ctx).Seq(seq.Range(0, 5)).
			ReduceCtx(0, func(_ context.Context, item, acc int) (int, error) {
				order = append(order, item)
				return acc + item, nil
			}, chunkflow.WithParallel(8), chunkflow.WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		require.NoError(t, err)
		assert.Equal(t, 10, total)
		assert.Equal(t, []int{0, 1, 2, 3, 4}, order, "must stay sequential and ordered")
		assert.Contains(t, buf.String(), "concurrent reduction is not supported")
	})

	t.Run("concurrency of 1 does not warn", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := chunkflow.NewIo(ctx).Seq(seq.Range(0, 5)).
			Opts(chunkflow.WithLogger(slog.New(slog.NewTextHandler(&buf, nil)))).
			ReduceCtx(0, func(_ context.Context, item, acc int) (int, error) { return acc + item, nil })
		require.NoError(t, err)
		assert.Empty(t, buf.String())
	})
}

func TestIoStream_PredicateErrors(t *testing.T) {
	ctx := t.Context()
	errPred := errors.New("predicate")
	failOn := func(n int) func(context.Context, int) (bool, error) {
		return func(_ context.Context, i int) (bool, error) {
			if i == n {
				return false, errPred
			}
			return true, nil
		}
	}

	t.Run("AllCtx returns the predicate error", func(t *testing.T) {
		var evaluated int
		ok, err := chunkflow.NewIo(ctx).Seq(seq.Range(0, 10)).
			AllCtx(func(ctx context.Context, i int) (bool, error) {
				evaluated++
				return failOn(3)(ctx, i)
			})
		require.ErrorIs(t, err, errPred)
		assert.False(t, ok)
		assert.Equal(t, 4, evaluated, "must stop at the failing predicate")
	})

	t.Run("AnyCtx returns the predicate error", func(t *testing.T) {
		ok, err := chunkflow.NewIo(ctx).Seq(seq.Range(0, 10)).
			AnyCtx(func(ctx context.Context, i int) (bool, error) {
				match, err := failOn(3)(ctx, i)
				return !match, err // nothing matches before the failure
			})
		require.ErrorIs(t, err, errPred)
		assert.False(t, ok)
	})

	t.Run("AllCtx and AnyCtx return upstream errors", func(t *testing.T) {
		_, err := chunkflow.NewIo(ctx).Seq2(errAfter(2, errBoom)).AllCtx(func(context.Context, int) (bool, error) { return true, nil })
		require.ErrorIs(t, err, errBoom)
		_, err = chunkflow.NewIo(ctx).Seq2(errAfter(2, errBoom)).AnyCtx(func(context.Context, int) (bool, error) { return false, nil })
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("FilterCtx predicate error is fatal for terminals", func(t *testing.T) {
		res, err := chunkflow.NewIo(ctx).Seq(seq.Range(0, 10)).FilterCtx(failOn(3)).Collect()
		require.ErrorIs(t, err, errPred)
		assert.Equal(t, []int{0, 1, 2}, res)
	})

	t.Run("FilterCtx predicate error can be tolerated by a breaker", func(t *testing.T) {
		for name, opts := range map[string][]chunkflow.Option{
			"sequential": nil,
			"parallel":   {chunkflow.WithParallel(2)},
		} {
			t.Run(name, func(t *testing.T) {
				// predicate fails on 3; upstream source fails after 6 items
				res, err := chunkflow.NewIo(ctx).Seq2(errAfter(6, errBoom)).
					FilterCtx(failOn(3), opts...).
					CircuitBreaker(100).
					Collect()
				require.NoError(t, err)
				assert.ElementsMatch(t, []int{0, 1, 2, 4, 5}, res)
			})
		}
	})

	t.Run("FilterCtx stops pulling once First has a match", func(t *testing.T) {
		var pulled int
		src := chunkflow.NewIo(ctx).Seq(func(yield func(int) bool) {
			for i := 0; ; i++ {
				pulled++
				if !yield(i) {
					return
				}
			}
		})
		v, ok, err := src.FilterCtx(func(_ context.Context, i int) (bool, error) { return i >= 2, nil }).First()
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 2, v)
		assert.Equal(t, 3, pulled)
	})
}

func TestIoStream_AllAnyEdgeCases(t *testing.T) {
	ctx := t.Context()
	isEven := func(i int) bool { return i%2 == 0 }
	isOdd := func(i int) bool { return !isEven(i) }

	empty := func() chunkflow.IoStream[int] { return chunkflow.NewIo(ctx).Seq(seq.Items[int]()) }
	// every element fails upstream and gets suppressed, so nothing reaches the terminal
	onlySuppressed := func() chunkflow.IoStream[int] {
		return chunkflow.NewIo(ctx).Seq(seq.Range(0, 5)).
			MapCtx(func(context.Context, int) (int, error) { return 0, errBoom }).
			CircuitBreaker(100)
	}

	for name, stream := range map[string]func() chunkflow.IoStream[int]{
		"empty":                  empty,
		"only suppressed errors": onlySuppressed,
	} {
		t.Run(name, func(t *testing.T) {
			// vacuous truth: no element can violate the predicate...
			all, err := stream().All(isEven)
			require.NoError(t, err)
			assert.True(t, all, "All on nothing must be true")

			all, err = stream().All(isOdd)
			require.NoError(t, err)
			assert.True(t, all, "...whatever the predicate is")

			// ...and no element can witness it
			any, err := stream().Any(isEven)
			require.NoError(t, err)
			assert.False(t, any, "Any on nothing must be false")

			any, err = stream().Any(isOdd)
			require.NoError(t, err)
			assert.False(t, any)
		})
	}

	t.Run("All(p) == !Any(!p) on non-empty streams", func(t *testing.T) {
		for _, items := range [][]int{{2, 4, 6}, {1, 2, 3}, {1, 3, 5}, {7}} {
			s := func() chunkflow.IoStream[int] { return chunkflow.NewIo(ctx).Seq(seq.Items(items...)) }
			all, err := s().All(isEven)
			require.NoError(t, err)
			anyNot, err := s().Any(isOdd)
			require.NoError(t, err)
			assert.Equal(t, all, !anyNot, "items=%v", items)
		}
	})
}

// TestIoStream_FailFastStopsEveryStage checks that when a terminal stops at an upstream
// error, each intermediate stage honours the consumer's stop and pulls nothing more.
func TestIoStream_FailFastStopsEveryStage(t *testing.T) {
	ctx := t.Context()

	// source: 0,1,2, error, 3,4,... — keeps producing after the error.
	// pulled is atomic because parallel stages read the source from a feeder goroutine.
	newSource := func(pulled *atomic.Int32) chunkflow.IoStream[int] {
		return chunkflow.NewIo(ctx).Seq2(func(yield func(int, error) bool) {
			for i := 0; ; i++ {
				pulled.Add(1)
				var e error
				if i == 3 {
					e = errBoom
				}
				if !yield(i, e) {
					return
				}
			}
		})
	}
	identity := func(_ context.Context, i int) (int, error) { return i, nil }
	truthy := func(context.Context, int) (bool, error) { return true, nil }

	stages := map[string]func(chunkflow.IoStream[int]) chunkflow.IoStream[int]{
		"MapCtx": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] { return s.MapCtx(identity) },
		"MapCtx parallel": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] {
			return s.MapCtx(identity, chunkflow.WithParallel(2))
		},
		"FilterCtx": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] { return s.FilterCtx(truthy) },
		"FilterCtx parallel": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] {
			return s.FilterCtx(truthy, chunkflow.WithParallel(2))
		},
		"Skip": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] { return s.Skip(1) },
		"TakeWhile": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] {
			return s.TakeWhile(func(int) bool { return true })
		},
		"SkipWhile": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] {
			return s.SkipWhile(func(i int) bool { return i < 1 })
		},
		"SkipWhile still skipping": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] {
			return s.SkipWhile(func(int) bool { return true })
		},
		"Take": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] { return s.Take(100) },
		"Chunk+IoFlatten": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] {
			return s.Chunk[[]int](2).Through(chunkflow.IoFlatten)
		},
		"CircuitBreaker(1)": func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] { return s.CircuitBreaker(1) },
	}

	for name, stage := range stages {
		t.Run(name, func(t *testing.T) {
			var pulled atomic.Int32
			_, err := newSource(&pulled).Through(stage).Collect()
			require.ErrorIs(t, err, errBoom)
			// Sequential stages pull exactly 4 items (0,1,2,err). Parallel ones with
			// concurrency c may prefetch up to c (inChan) + c (outChan) + c (in workers) + 1
			// (held by the feeder) more, plus one pull that may be in flight while the feeder
			// is still observing the cancellation. They must not run away beyond that.
			const c = 2
			n := int(pulled.Load())
			assert.GreaterOrEqual(t, n, 4)
			assert.LessOrEqual(t, n, 4+3*c+2, "stage kept pulling after the consumer stopped")
		})
	}
}

func TestIoStream_ChunkRejectsInvalidSize(t *testing.T) {
	_, err := chunkflow.NewIo(t.Context()).Seq(seq.Items(1, 2)).Chunk(0).Collect()
	require.ErrorContains(t, err, "chunk size must be >= 1")
}
