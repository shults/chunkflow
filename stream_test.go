package chunkflow_test

import (
	"strconv"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/stretchr/testify/assert"
)

func TestStream_Generators(t *testing.T) {
	t.Run("FromItems should yield all provided arguments", func(t *testing.T) {
		res := chunkflow.FromItems(1, 2, 3).Collect()
		assert.Equal(t, []int{1, 2, 3}, res)
	})

	t.Run("FromRange should yield exact boundaries", func(t *testing.T) {
		res := chunkflow.FromRange(5, 8).Collect()
		assert.Equal(t, []int{5, 6, 7}, res)
	})

	t.Run("Const should yield same value and terminate on Take", func(t *testing.T) {
		res := chunkflow.Const("A").Take(3).Collect()
		assert.Equal(t, []string{"A", "A", "A"}, res)
	})

	t.Run("Numbers should generate infinite sequence and break correctly", func(t *testing.T) {
		res := chunkflow.Numbers(10).Take(4).Collect()
		assert.Equal(t, []int{10, 11, 12, 13}, res)
	})
}

func TestStream_Transformations(t *testing.T) {
	t.Run("Map should transform types correctly", func(t *testing.T) {
		res := chunkflow.FromItems(1, 2, 3).
			Map(func(i int) string {
				return strconv.Itoa(i) + "x"
			}).
			Collect()
		assert.Equal(t, []string{"1x", "2x", "3x"}, res)
	})

	t.Run("Filter should drop non-matching items", func(t *testing.T) {
		res := chunkflow.FromRange(1, 10).
			Filter(func(i int) bool {
				return i%2 == 0
			}).
			Collect()
		assert.Equal(t, []int{2, 4, 6, 8}, res)
	})

	t.Run("Chunk should group elements and handle remainders", func(t *testing.T) {
		res := chunkflow.FromRange(1, 6).Chunk(2).Collect()
		assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, res)
	})

	t.Run("Chunk should panic on invalid size", func(t *testing.T) {
		assert.PanicsWithValue(t, "invalid chunk size", func() {
			chunkflow.FromItems(1, 2).Chunk(0).Exec()
		})
	})
}

func TestStream_Slicing(t *testing.T) {
	t.Run("Skip should bypass exact number of elements", func(t *testing.T) {
		res := chunkflow.FromRange(1, 10).Skip(5).Collect()
		assert.Equal(t, []int{6, 7, 8, 9}, res)
	})

	t.Run("Skip+Take allows for precise pagination", func(t *testing.T) {
		res := chunkflow.Numbers(0).Skip(10).Take(5).Collect()
		assert.Equal(t, []int{10, 11, 12, 13, 14}, res)
	})
}

func TestStream_TerminalOperations(t *testing.T) {
	t.Run("Reduce should aggregate values", func(t *testing.T) {
		sum := chunkflow.FromRange(1, 5).Reduce(0, func(item, acc int) int {
			return acc + item
		})
		assert.Equal(t, 10, sum) // 1+2+3+4
	})

	t.Run("Any should short-circuit true on first match", func(t *testing.T) {
		evaluatedItems := 0
		res := chunkflow.Numbers(1).
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
		res := chunkflow.FromItems(2, 4, 5, 8).
			Map(func(i int) int {
				evaluatedItems++
				return i
			}).
			All(func(i int) bool { return i%2 == 0 })

		assert.False(t, res)
		assert.Equal(t, 3, evaluatedItems, "All failed to short-circuit the pipeline")
	})

	t.Run("First on non-empty stream returns element", func(t *testing.T) {
		val, ok := chunkflow.FromItems(99, 100).First()
		assert.True(t, ok)
		assert.Equal(t, 99, val)
	})

	t.Run("First on empty stream returns false", func(t *testing.T) {
		val, ok := chunkflow.FromItems[int]().First()
		assert.False(t, ok)
		assert.Equal(t, 0, val)
	})

	t.Run("Last on non-empty stream returns final element", func(t *testing.T) {
		val, ok := chunkflow.FromItems(1, 2, 99).Last()
		assert.True(t, ok)
		assert.Equal(t, 99, val)
	})
}

func TestStream_FlattenAndThrough(t *testing.T) {
	t.Run("Flatten should unnest stream of slices into flat stream", func(t *testing.T) {
		chunkedStream := chunkflow.FromItems([]int{1, 2}, []int{3, 4}, []int{5})
		flatStream := chunkflow.Flatten(chunkedStream)

		res := flatStream.Collect()
		assert.Equal(t, []int{1, 2, 3, 4, 5}, res)
	})

	t.Run("Through should pipe stream seamlessly maintaining fluent API", func(t *testing.T) {
		res := chunkflow.FromRange(1, 6).
			Chunk[[]int](2).                 // Chunking: [1, 2], [3, 4], [5]
			Through(chunkflow.Flatten[int]). // Flatten: 1, 2, 3, 4, 5
			Map(func(i int) int {            // Transform: 10, 20, 30, 40, 50
				return i * 10
			}).
			Collect()

		assert.Equal(t, []int{10, 20, 30, 40, 50}, res)
	})

	t.Run("Flatten should safely ignore empty chunks", func(t *testing.T) {
		chunkedStream := chunkflow.FromItems([]int{1}, []int{}, []int{2, 3}, nil, []int{4})

		res := chunkflow.Flatten(chunkedStream).Collect()
		assert.Equal(t, []int{1, 2, 3, 4}, res, "Flatten failed to skip empty/nil slices")
	})

	t.Run("Through can apply arbitrary stream transformations", func(t *testing.T) {
		takeThird := func(s chunkflow.Stream[string]) chunkflow.Stream[string] {
			return s.Skip(2).Take(1)
		}

		res := chunkflow.FromItems("a", "b", "c", "d").
			Through(takeThird).
			Collect()

		assert.Equal(t, []string{"c"}, res)
	})
}
