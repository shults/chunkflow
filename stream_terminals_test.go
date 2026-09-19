package chunkflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/policy"
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

func TestStream_Exec(t *testing.T) {
	ctx := t.Context()

	t.Run("exhausts the stream and triggers side effects", func(t *testing.T) {
		var seen int
		err := chunkflow.
			New(ctx).Seq(seq.Range(0, 5)).
			Map(func(i int) int { seen++; return i }).
			Drain()
		require.NoError(t, err)
		assert.Equal(t, 5, seen)
	})

	t.Run("is a no-op on an empty stream", func(t *testing.T) {
		require.NoError(t, chunkflow.New(ctx).Seq(seq.Items[int]()).Drain())
	})

	t.Run("returns the first upstream error and stops consuming", func(t *testing.T) {
		var pulled int
		err := chunkflow.
			New(ctx).Seq2(errAfter(3, errBoom)).
			Map(func(i int) int { pulled++; return i }).
			Drain()
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, 3, pulled)
	})

	t.Run("returns a cancelled context as error", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, chunkflow.New(cctx).Seq(seq.Numbers(0)).Drain(), context.Canceled)
	})
}

func TestStream_Reduce(t *testing.T) {
	ctx := t.Context()
	sum := func(acc, item int) int { return acc + item }

	t.Run("aggregates all values starting from init", func(t *testing.T) {
		total, err := chunkflow.New(ctx).Seq(seq.Range(1, 5)).Reduce(100, sum)
		require.NoError(t, err)
		assert.Equal(t, 110, total)
	})

	t.Run("returns init for an empty stream", func(t *testing.T) {
		total, err := chunkflow.New(ctx).Seq(seq.Items[int]()).Reduce(42, sum)
		require.NoError(t, err)
		assert.Equal(t, 42, total)
	})

	t.Run("passes accumulator first and item second", func(t *testing.T) {
		var order [][2]int
		_, err := chunkflow.New(ctx).Seq(seq.Items(10, 20)).Reduce(1, func(acc, item int) int {
			order = append(order, [2]int{acc, item})
			return acc + item
		})
		require.NoError(t, err)
		assert.Equal(t, [][2]int{{1, 10}, {11, 20}}, order)
	})

	t.Run("accumulator type is independent of the element type", func(t *testing.T) {
		lengths, err := chunkflow.New(ctx).Seq(seq.Items("go", "iter", "seq")).
			Reduce(0, func(acc int, s string) int { return acc + len(s) })
		require.NoError(t, err)
		assert.Equal(t, 9, lengths)

		index, err := chunkflow.New(ctx).Seq(seq.Items("a", "bb", "a")).
			Reduce(map[string]int{}, func(acc map[string]int, s string) map[string]int {
				acc[s]++
				return acc
			})
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"a": 2, "bb": 1}, index)
	})

	t.Run("returns the partial accumulator together with an upstream error", func(t *testing.T) {
		total, err := chunkflow.New(ctx).Seq2(errAfter(3, errBoom)).Reduce(0, sum) // 0+1+2
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, 3, total)
	})

	t.Run("skips suppressed errors", func(t *testing.T) {
		total, err := tolerant(ctx).Reduce(0, sum) // 0,1,5,6,7,8,9
		require.NoError(t, err)
		assert.Equal(t, 36, total)
	})
}

func TestStream_ReduceCtx(t *testing.T) {
	ctx := t.Context()

	t.Run("receives the stream context", func(t *testing.T) {
		type key struct{}
		cctx := context.WithValue(ctx, key{}, 42)

		got, err := chunkflow.New(cctx).Seq(seq.Items(1)).
			ReduceCtx(0, func(ctx context.Context, _, _ int) (int, error) {
				v, _ := ctx.Value(key{}).(int)
				return v, nil
			})
		require.NoError(t, err)
		assert.Equal(t, 42, got)
	})

	t.Run("callback error is returned with the accumulator so far", func(t *testing.T) {
		errCb := errors.New("callback")
		total, err := chunkflow.New(ctx).Seq(seq.Range(1, 10)).
			ReduceCtx(0, func(_ context.Context, acc, item int) (int, error) {
				if item == 4 {
					return acc, errCb
				}
				return acc + item, nil
			})
		require.ErrorIs(t, err, errCb)
		assert.Equal(t, 6, total, "1+2+3 accumulated before the failure")
	})

	t.Run("folds sequentially in source order", func(t *testing.T) {
		var order []int
		total, err := chunkflow.New(ctx).Seq(seq.Range(0, 5)).
			Opts(chunkflow.WithParallel(8)). // pipeline default must not leak into the fold
			ReduceCtx(0, func(_ context.Context, acc, item int) (int, error) {
				order = append(order, item)
				return acc + item, nil
			})
		require.NoError(t, err)
		assert.Equal(t, 10, total)
		assert.Equal(t, []int{0, 1, 2, 3, 4}, order)
	})
}

func TestStream_PredicateErrors(t *testing.T) {
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
		ok, err := chunkflow.New(ctx).Seq(seq.Range(0, 10)).
			AllCtx(func(ctx context.Context, i int) (bool, error) {
				evaluated++
				return failOn(3)(ctx, i)
			})
		require.ErrorIs(t, err, errPred)
		assert.False(t, ok)
		assert.Equal(t, 4, evaluated, "must stop at the failing predicate")
	})

	t.Run("AnyCtx returns the predicate error", func(t *testing.T) {
		ok, err := chunkflow.New(ctx).Seq(seq.Range(0, 10)).
			AnyCtx(func(ctx context.Context, i int) (bool, error) {
				match, err := failOn(3)(ctx, i)
				return !match, err // nothing matches before the failure
			})
		require.ErrorIs(t, err, errPred)
		assert.False(t, ok)
	})

	t.Run("AllCtx and AnyCtx return upstream errors", func(t *testing.T) {
		_, err := chunkflow.New(ctx).Seq2(errAfter(2, errBoom)).AllCtx(func(context.Context, int) (bool, error) { return true, nil })
		require.ErrorIs(t, err, errBoom)
		_, err = chunkflow.New(ctx).Seq2(errAfter(2, errBoom)).AnyCtx(func(context.Context, int) (bool, error) { return false, nil })
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("FilterCtx predicate error is fatal for terminals", func(t *testing.T) {
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 10)).FilterCtx(failOn(3)).Collect()
		require.ErrorIs(t, err, errPred)
		assert.Equal(t, []int{0, 1, 2}, res)
	})

	t.Run("FilterCtx predicate error can be tolerated by a breaker", func(t *testing.T) {
		for name, opts := range map[string][]chunkflow.StepOption{
			"sequential": nil,
			"parallel":   {chunkflow.WithParallel(2)},
		} {
			t.Run(name, func(t *testing.T) {
				// predicate fails on 3; upstream source fails after 6 items
				res, err := chunkflow.New(ctx).Seq2(errAfter(6, errBoom)).
					FilterCtx(failOn(3), opts...).
					Through(policy.CircuitBreaker[int](100)).
					Collect()
				require.NoError(t, err)
				assert.ElementsMatch(t, []int{0, 1, 2, 4, 5}, res)
			})
		}
	})

	t.Run("FilterCtx stops pulling once First has a match", func(t *testing.T) {
		var pulled int
		src := chunkflow.New(ctx).Seq(func(yield func(int) bool) {
			for i := 0; ; i++ {
				pulled++
				if !yield(i) {
					return
				}
			}
		})
		v, err := src.FilterCtx(func(_ context.Context, i int) (bool, error) { return i >= 2, nil }).First()
		require.NoError(t, err)
		assert.Equal(t, 2, v)
		assert.Equal(t, 3, pulled)
	})
}

func TestStream_AllAnyEdgeCases(t *testing.T) {
	ctx := t.Context()
	isEven := func(i int) bool { return i%2 == 0 }
	isOdd := func(i int) bool { return !isEven(i) }

	empty := func() chunkflow.Stream[int] { return chunkflow.New(ctx).Seq(seq.Items[int]()) }
	// every element fails upstream and gets suppressed, so nothing reaches the terminal
	onlySuppressed := func() chunkflow.Stream[int] {
		return chunkflow.New(ctx).Seq(seq.Range(0, 5)).
			MapCtx(func(context.Context, int) (int, error) { return 0, errBoom }).
			Through(policy.CircuitBreaker[int](100))
	}

	for name, stream := range map[string]func() chunkflow.Stream[int]{
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
			s := func() chunkflow.Stream[int] { return chunkflow.New(ctx).Seq(seq.Items(items...)) }
			all, err := s().All(isEven)
			require.NoError(t, err)
			anyNot, err := s().Any(isOdd)
			require.NoError(t, err)
			assert.Equal(t, all, !anyNot, "items=%v", items)
		}
	})
}

// TestStream_FailFastStopsEveryStage checks that when a terminal stops at an upstream
// error, each intermediate stage honours the consumer's stop and pulls nothing more.
func TestStream_FailFastStopsEveryStage(t *testing.T) {
	ctx := t.Context()

	// source: 0,1,2, error, 3,4,... — keeps producing after the error.
	// pulled is atomic because parallel stages read the source from a feeder goroutine.
	newSource := func(pulled *atomic.Int32) chunkflow.Stream[int] {
		return chunkflow.New(ctx).Seq2(func(yield func(int, error) bool) {
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

	stages := map[string]func(chunkflow.Stream[int]) chunkflow.Stream[int]{
		"MapCtx": func(s chunkflow.Stream[int]) chunkflow.Stream[int] { return s.MapCtx(identity) },
		"MapCtx parallel": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
			return s.MapCtx(identity, chunkflow.WithParallel(2))
		},
		"FilterCtx": func(s chunkflow.Stream[int]) chunkflow.Stream[int] { return s.FilterCtx(truthy) },
		"FilterCtx parallel": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
			return s.FilterCtx(truthy, chunkflow.WithParallel(2))
		},
		"Skip": func(s chunkflow.Stream[int]) chunkflow.Stream[int] { return s.Skip(1) },
		"TakeWhile": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
			return s.TakeWhile(func(int) bool { return true })
		},
		"SkipWhile": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
			return s.SkipWhile(func(i int) bool { return i < 1 })
		},
		"SkipWhile still skipping": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
			return s.SkipWhile(func(int) bool { return true })
		},
		"Take": func(s chunkflow.Stream[int]) chunkflow.Stream[int] { return s.Take(100) },
		"Chunk+Flatten": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
			return s.Chunk[[]int](2).Through(chunkflow.Flatten)
		},
		"CircuitBreaker(1)": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
			return s.Through(policy.CircuitBreaker[int](1))
		},
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

func TestStream_ChunkRejectsInvalidSize(t *testing.T) {
	_, err := chunkflow.New(t.Context()).Seq(seq.Items(1, 2)).Chunk(0).Collect()
	require.ErrorContains(t, err, "chunk size must be >= 1")
}

func TestStream_ReduceBy(t *testing.T) {
	ctx := t.Context()
	parity := func(i int) string {
		if i%2 == 0 {
			return "even"
		}
		return "odd"
	}
	appendInt := func(acc []int, i int) []int { return append(acc, i) }

	t.Run("groups in source order and starts every key from init", func(t *testing.T) {
		groups, err := chunkflow.New(ctx).Seq(seq.Range(0, 7)).ReduceBy(parity, nil, appendInt)
		require.NoError(t, err)
		assert.Equal(t, map[string][]int{"even": {0, 2, 4, 6}, "odd": {1, 3, 5}}, groups)

		sums, err := chunkflow.New(ctx).Seq(seq.Range(0, 7)).
			ReduceBy(parity, 100, func(acc, i int) int { return acc + i })
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"even": 112, "odd": 109}, sums, "init is applied per key, not once")
	})

	t.Run("empty stream gives an empty, usable map", func(t *testing.T) {
		groups, err := chunkflow.New(ctx).Seq(seq.Items[int]()).ReduceBy(parity, nil, appendInt)
		require.NoError(t, err)
		require.NotNil(t, groups)
		assert.Empty(t, groups)
	})

	t.Run("error returns the map built so far", func(t *testing.T) {
		groups, err := chunkflow.New(ctx).Seq2(errAfter(3, errBoom)).ReduceBy(parity, nil, appendInt) // 0,1,2 then boom
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, map[string][]int{"even": {0, 2}, "odd": {1}}, groups)
	})

	t.Run("suppressed errors are skipped", func(t *testing.T) {
		groups, err := tolerant(ctx).ReduceBy(parity, nil, appendInt) // 0,1,5,6,7,8,9
		require.NoError(t, err)
		assert.Equal(t, map[string][]int{"even": {0, 6, 8}, "odd": {1, 5, 7, 9}}, groups)
	})

	t.Run("a pipeline-wide WithParallel does not reach the fold", func(t *testing.T) {
		var order []int
		_, err := chunkflow.New(ctx).Seq(seq.Range(0, 20)).
			Opts(chunkflow.WithParallel(8)).
			ReduceBy(parity, nil, func(acc []int, i int) []int { order = append(order, i); return append(acc, i) })
		require.NoError(t, err)
		assert.IsIncreasing(t, order)
	})
}

func TestStream_ReduceByCtx(t *testing.T) {
	ctx := t.Context()
	key := func(i int) int { return i % 3 }

	t.Run("receives the stream context", func(t *testing.T) {
		type ctxKey struct{}
		cctx := context.WithValue(ctx, ctxKey{}, "marker")
		seen := 0
		_, err := chunkflow.New(cctx).Seq(seq.Range(0, 3)).
			ReduceByCtx(key, 0, func(ctx context.Context, acc, i int) (int, error) {
				if ctx.Value(ctxKey{}) == "marker" {
					seen++
				}
				return acc + i, nil
			})
		require.NoError(t, err)
		assert.Equal(t, 3, seen)
	})

	t.Run("a callback error stops the fold, keeps the partial map and reaches WithOnError", func(t *testing.T) {
		var reported []error
		groups, err := chunkflow.New(ctx).Seq(seq.Range(0, 10)).
			Opts(chunkflow.WithOnError(func(err error) { reported = append(reported, err) })).
			ReduceByCtx(key, 0, func(_ context.Context, acc, i int) (int, error) {
				if i == 4 {
					return acc, errBoom
				}
				return acc + 1, nil
			})
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, map[int]int{0: 2, 1: 1, 2: 1}, groups, "0,1,2,3 counted; 4 failed and its group keeps the returned acc")
		assert.Equal(t, []error{errBoom}, reported)
	})

	t.Run("Suppress inside the fold skips the item but keeps the group", func(t *testing.T) {
		groups, err := chunkflow.New(ctx).Seq(seq.Range(0, 6)).
			ReduceByCtx(key, 0, func(_ context.Context, acc, i int) (int, error) {
				if i == 5 {
					return acc, chunkflow.Suppress(errBoom)
				}
				return acc + 1, nil
			})
		require.ErrorIs(t, err, errBoom, "a suppressed error returned by a terminal callback is still the terminal's error")
		assert.Equal(t, map[int]int{0: 2, 1: 2, 2: 1}, groups)
	})
}
