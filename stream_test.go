package chunkflow_test

import (
	"strconv"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
)

func TestStream_Generators(t *testing.T) {
	t.Run("seq.Items should yield all provided arguments", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Items(1, 2, 3)).Collect()
		assert.Equal(t, []int{1, 2, 3}, res)
	})

	t.Run("seq.Range should yield half-open boundaries", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Range(5, 8)).Collect()
		assert.Equal(t, []int{5, 6, 7}, res)
	})

	t.Run("seq.Const should yield same value and terminate on Take", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Const("A")).Take(3).Collect()
		assert.Equal(t, []string{"A", "A", "A"}, res)
	})

	t.Run("seq.Numbers should generate infinite sequence and break correctly", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Numbers(10)).Take(4).Collect()
		assert.Equal(t, []int{10, 11, 12, 13}, res)
	})
}

func TestStream_Transformations(t *testing.T) {
	t.Run("Map should transform types correctly", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Items(1, 2, 3)).
			Map(func(i int) string {
				return strconv.Itoa(i) + "x"
			}).
			Collect()
		assert.Equal(t, []string{"1x", "2x", "3x"}, res)
	})

	t.Run("Tap should observe every element without changing the stream", func(t *testing.T) {
		var seen []int
		res := chunkflow.NewStream(seq.Range(1, 4)).
			Tap(func(i int) { seen = append(seen, i) }).
			Map(func(i int) int { return i * 10 }).
			Collect()
		assert.Equal(t, []int{10, 20, 30}, res)
		assert.Equal(t, []int{1, 2, 3}, seen)
	})

	t.Run("Tap is lazy and honours short-circuiting", func(t *testing.T) {
		calls := 0
		s := chunkflow.NewStream(seq.Numbers(0)).Tap(func(int) { calls++ })
		assert.Equal(t, 0, calls, "nothing may run before a terminal operation")
		_, _ = s.Skip(2).First()
		assert.Equal(t, 3, calls)
	})

	t.Run("Filter should drop non-matching items", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Range(1, 10)).
			Filter(func(i int) bool {
				return i%2 == 0
			}).
			Collect()
		assert.Equal(t, []int{2, 4, 6, 8}, res)
	})

	t.Run("Compact drops only consecutive duplicates", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Items(1, 1, 2, 2, 2, 3, 1, 1)).
			Through(chunkflow.Compact).
			Collect()
		assert.Equal(t, []int{1, 2, 3, 1}, res, "1 reappears because it is not adjacent to the first run")
	})

	t.Run("Compact on empty and single-element streams", func(t *testing.T) {
		assert.Empty(t, chunkflow.Compact(chunkflow.NewStream(seq.Items[int]())).Collect())
		assert.Equal(t, []int{7}, chunkflow.Compact(chunkflow.NewStream(seq.Items(7))).Collect())
	})

	t.Run("CompactFunc compares with a custom equality", func(t *testing.T) {
		type row struct{ key, payload string }
		res := chunkflow.NewStream(seq.Items(
			row{"a", "1"}, row{"a", "2"}, row{"b", "3"}, row{"b", "4"}, row{"a", "5"},
		)).
			CompactFunc(func(x, y row) bool { return x.key == y.key }).
			Collect()
		assert.Equal(t, []row{{"a", "1"}, {"b", "3"}, {"a", "5"}}, res, "the first of each run is kept")
	})

	t.Run("Chunk should group elements and handle remainders", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Range(1, 6)).Chunk(2).Collect()
		assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, res)
	})

	t.Run("Chunk should panic on invalid size", func(t *testing.T) {
		assert.PanicsWithValue(t, "invalid chunk size", func() {
			chunkflow.NewStream(seq.Items(1, 2)).Chunk(0).Exec()
		})
	})
}

func TestStream_Slicing(t *testing.T) {
	t.Run("Skip should bypass exact number of elements", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Range(1, 10)).Skip(5).Collect()
		assert.Equal(t, []int{6, 7, 8, 9}, res)
	})

	t.Run("TakeWhile stops at the first mismatch and stops pulling", func(t *testing.T) {
		pulled := 0
		res := chunkflow.NewStream(seq.Items(1, 2, 3, 10, 4, 5)).
			Tap(func(int) { pulled++ }).
			TakeWhile(func(i int) bool { return i < 4 }).
			Collect()
		assert.Equal(t, []int{1, 2, 3}, res)
		assert.Equal(t, 4, pulled, "the failing element is pulled, nothing after it")
	})

	t.Run("TakeWhile on an infinite source", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Numbers(0)).TakeWhile(func(i int) bool { return i*i < 30 }).Collect()
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, res)
	})

	t.Run("SkipWhile drops the prefix and then stops evaluating", func(t *testing.T) {
		evaluated := 0
		res := chunkflow.NewStream(seq.Items(1, 2, 3, 10, 4, 5)).
			SkipWhile(func(i int) bool { evaluated++; return i < 4 }).
			Collect()
		assert.Equal(t, []int{10, 4, 5}, res)
		assert.Equal(t, 4, evaluated, "predicate must not run after the first false")
	})

	t.Run("TakeWhile and SkipWhile on empty and all-matching streams", func(t *testing.T) {
		lt10 := func(i int) bool { return i < 10 }
		assert.Empty(t, chunkflow.NewStream(seq.Items[int]()).TakeWhile(lt10).Collect())
		assert.Empty(t, chunkflow.NewStream(seq.Items[int]()).SkipWhile(lt10).Collect())
		assert.Equal(t, []int{1, 2}, chunkflow.NewStream(seq.Items(1, 2)).TakeWhile(lt10).Collect())
		assert.Empty(t, chunkflow.NewStream(seq.Items(1, 2)).SkipWhile(lt10).Collect())
	})

	t.Run("Skip+Take allows for precise pagination", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Numbers(0)).Skip(10).Take(5).Collect()
		assert.Equal(t, []int{10, 11, 12, 13, 14}, res)
	})
}

func TestStream_TerminalOperations(t *testing.T) {
	t.Run("Reduce should aggregate values", func(t *testing.T) {
		sum := chunkflow.NewStream(seq.Range(1, 5)).Reduce(0, func(item, acc int) int {
			return acc + item
		})
		assert.Equal(t, 10, sum) // 1+2+3+4
	})

	t.Run("Any should short-circuit true on first match", func(t *testing.T) {
		evaluatedItems := 0
		res := chunkflow.NewStream(seq.Numbers(1)).
			Map(func(i int) int {
				evaluatedItems++
				return i
			}).
			Any(func(i int) bool { return i == 3 })

		assert.True(t, res)
		assert.Equal(t, 3, evaluatedItems, "Any failed to short-circuit the pipeline")
	})

	t.Run("All should short-circuit false on first mismatch", func(t *testing.T) {
		evaluatedItems := 0
		res := chunkflow.NewStream(seq.Items(2, 4, 5, 8)).
			Map(func(i int) int {
				evaluatedItems++
				return i
			}).
			All(func(i int) bool { return i%2 == 0 })

		assert.False(t, res)
		assert.Equal(t, 3, evaluatedItems, "All failed to short-circuit the pipeline")
	})

	t.Run("All returns true when every element matches", func(t *testing.T) {
		assert.True(t, chunkflow.NewStream(seq.Items(2, 4, 6)).All(func(i int) bool { return i%2 == 0 }))
		assert.True(t, chunkflow.NewStream(seq.Items[int]()).All(func(int) bool { return false }), "vacuous truth on empty stream")
	})

	t.Run("Any returns false when nothing matches", func(t *testing.T) {
		assert.False(t, chunkflow.NewStream(seq.Items(1, 3, 5)).Any(func(i int) bool { return i%2 == 0 }))
		assert.False(t, chunkflow.NewStream(seq.Items[int]()).Any(func(int) bool { return true }))
	})

	t.Run("First on non-empty stream returns element", func(t *testing.T) {
		val, ok := chunkflow.NewStream(seq.Items(99, 100)).First()
		assert.True(t, ok)
		assert.Equal(t, 99, val)
	})

	t.Run("First on empty stream returns false", func(t *testing.T) {
		val, ok := chunkflow.NewStream(seq.Items[int]()).First()
		assert.False(t, ok)
		assert.Equal(t, 0, val)
	})

	t.Run("Last on non-empty stream returns final element", func(t *testing.T) {
		val, ok := chunkflow.NewStream(seq.Items(1, 2, 99)).Last()
		assert.True(t, ok)
		assert.Equal(t, 99, val)
	})

	t.Run("Seq should return underlying iterator", func(t *testing.T) {
		s := chunkflow.NewStream(seq.Items(1, 2))
		seq := s.Seq()
		var res []int
		for val := range seq {
			res = append(res, val)
		}
		assert.Equal(t, []int{1, 2}, res)
	})

	t.Run("Count should return number of elements", func(t *testing.T) {
		assert.Equal(t, 4, chunkflow.NewStream(seq.Range(1, 10)).Filter(func(i int) bool { return i%2 == 0 }).Count())
		assert.Equal(t, 0, chunkflow.NewStream(seq.Items[int]()).Count())
	})

	t.Run("ForEach should execute side effect", func(t *testing.T) {
		var sum int
		chunkflow.NewStream(seq.Items(1, 2, 3)).ForEach(func(i int) {
			sum += i
		})
		assert.Equal(t, 6, sum)
	})

	t.Run("Exec should exhaust stream", func(t *testing.T) {
		var count int
		chunkflow.NewStream(seq.Items(1, 2, 3)).
			Map(func(i int) int {
				count++
				return i
			}).
			Exec()
		assert.Equal(t, 3, count)
	})
}

func TestStream_FlattenAndThrough(t *testing.T) {
	t.Run("Flatten should unnest stream of slices into flat stream", func(t *testing.T) {
		chunkedStream := chunkflow.NewStream(seq.Items([]int{1, 2}, []int{3, 4}, []int{5}))
		flatStream := chunkflow.Flatten(chunkedStream)

		res := flatStream.Collect()
		assert.Equal(t, []int{1, 2, 3, 4, 5}, res)
	})

	t.Run("Through should pipe stream seamlessly maintaining fluent API", func(t *testing.T) {
		res := chunkflow.NewStream(seq.Range(1, 6)).
			Chunk[[]int](2).                 // Chunking: [1, 2], [3, 4], [5]
			Through(chunkflow.Flatten[int]). // Flatten: 1, 2, 3, 4, 5
			Map(func(i int) int {            // Transform: 10, 20, 30, 40, 50
				return i * 10
			}).
			Collect()

		assert.Equal(t, []int{10, 20, 30, 40, 50}, res)
	})

	t.Run("Flatten should safely ignore empty chunks", func(t *testing.T) {
		chunkedStream := chunkflow.NewStream(seq.Items([]int{1}, []int{}, []int{2, 3}, nil, []int{4}))

		res := chunkflow.Flatten(chunkedStream).Collect()
		assert.Equal(t, []int{1, 2, 3, 4}, res, "Flatten failed to skip empty/nil slices")
	})

	t.Run("Through can apply arbitrary stream transformations", func(t *testing.T) {
		takeThird := func(s chunkflow.Stream[string]) chunkflow.Stream[string] {
			return s.Skip(2).Take(1)
		}

		res := chunkflow.NewStream(seq.Items("a", "b", "c", "d")).
			Through(takeThird).
			Collect()

		assert.Equal(t, []string{"c"}, res)
	})
}

// TestStream_ShortCircuitPropagates verifies that every intermediate operation stops
// pulling from an infinite source once the consumer has what it needs.
func TestStream_ShortCircuitPropagates(t *testing.T) {
	stages := map[string]func(chunkflow.Stream[int]) chunkflow.Stream[int]{
		"Map":    func(s chunkflow.Stream[int]) chunkflow.Stream[int] { return s.Map(func(i int) int { return i }) },
		"Filter": func(s chunkflow.Stream[int]) chunkflow.Stream[int] { return s.Filter(func(int) bool { return true }) },
		"Take":   func(s chunkflow.Stream[int]) chunkflow.Stream[int] { return s.Take(1000) },
		"Skip":   func(s chunkflow.Stream[int]) chunkflow.Stream[int] { return s.Skip(1) },
		"TakeWhile": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
			return s.TakeWhile(func(int) bool { return true })
		},
		"SkipWhile": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
			return s.SkipWhile(func(i int) bool { return i < 1 })
		},
		"Chunk+Flatten": func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
			return s.Chunk[[]int](3).Through(chunkflow.Flatten)
		},
		"Compact": func(s chunkflow.Stream[int]) chunkflow.Stream[int] { return s.Through(chunkflow.Compact) },
	}

	for name, stage := range stages {
		t.Run(name, func(t *testing.T) {
			pulled := 0
			src := chunkflow.NewStream(func(yield func(int) bool) {
				for i := 0; ; i++ {
					pulled++
					if !yield(i) {
						return
					}
				}
			})

			_, ok := src.Through(stage).First()
			assert.True(t, ok)
			assert.LessOrEqual(t, pulled, 4, "stage kept pulling after First() returned")
		})
	}

	t.Run("Chunk stops after the consumer takes one chunk", func(t *testing.T) {
		pulled := 0
		src := chunkflow.NewStream(func(yield func(int) bool) {
			for i := 0; ; i++ {
				pulled++
				if !yield(i) {
					return
				}
			}
		})
		chunk, ok := src.Chunk(3).First()
		assert.True(t, ok)
		assert.Equal(t, []int{0, 1, 2}, chunk)
		assert.Equal(t, 3, pulled)
	})
}

func TestStream_Concat(t *testing.T) {
	t.Run("emits streams one after another", func(t *testing.T) {
		res := chunkflow.Concat(
			chunkflow.NewStream(seq.Items(1, 2)),
			chunkflow.NewStream(seq.Items[int]()),
			chunkflow.NewStream(seq.Items(3)),
		).Collect()
		assert.Equal(t, []int{1, 2, 3}, res)
	})

	t.Run("with no arguments is empty", func(t *testing.T) {
		assert.Empty(t, chunkflow.Concat[int]().Collect())
	})

	t.Run("does not touch later streams while short-circuiting in an earlier one", func(t *testing.T) {
		secondPulled := 0
		second := chunkflow.NewStream(seq.Numbers(100)).Tap(func(int) { secondPulled++ })
		res := chunkflow.Concat(chunkflow.NewStream(seq.Numbers(0)), second).Take(3).Collect()
		assert.Equal(t, []int{0, 1, 2}, res)
		assert.Equal(t, 0, secondPulled)
	})

	t.Run("can be re-iterated", func(t *testing.T) {
		s := chunkflow.Concat(chunkflow.NewStream(seq.Items(1)), chunkflow.NewStream(seq.Items(2)))
		assert.Equal(t, []int{1, 2}, s.Collect())
		assert.Equal(t, []int{1, 2}, s.Collect())
	})
}
