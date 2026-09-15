package chunkflow_test

import (
	"fmt"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
)

func ExampleNewStream() {
	evens := chunkflow.NewStream(seq.Range(0, 10)).
		Filter(func(i int) bool { return i%2 == 0 }).
		Collect()

	fmt.Println(evens)
	// Output: [0 2 4 6 8]
}

func ExampleStream_Map() {
	words := chunkflow.NewStream(seq.Items("a", "bb", "ccc")).
		Map(func(s string) int { return len(s) }).
		Collect()

	fmt.Println(words)
	// Output: [1 2 3]
}

func ExampleStream_Take_infinite() {
	// Numbers is infinite; Take short-circuits so only 5 elements are ever generated.
	first := chunkflow.NewStream(seq.Numbers(1)).
		Map(func(i int) int { return i * i }).
		Take(5).
		Collect()

	fmt.Println(first)
	// Output: [1 4 9 16 25]
}

func ExampleStream_Chunk() {
	chunks := chunkflow.NewStream(seq.Range(1, 8)).
		Chunk(3).
		Collect()

	fmt.Println(chunks)
	// Output: [[1 2 3] [4 5 6] [7]]
}

func ExampleFlatten() {
	// Flatten changes the element type, so it is a top-level function.
	// Through keeps the pipeline readable left to right.
	flat := chunkflow.NewStream(seq.Range(1, 8)).
		Chunk[[]int](3).
		Through(chunkflow.Flatten).
		Collect()

	fmt.Println(flat)
	// Output: [1 2 3 4 5 6 7]
}

func ExampleStream_Reduce() {
	sum := chunkflow.NewStream(seq.RangeInclusive(1, 10)).
		Reduce(0, func(item, acc int) int { return acc + item })

	fmt.Println(sum)
	// Output: 55
}

func ExampleStream_All() {
	s := chunkflow.NewStream(seq.Items[int]())

	// Vacuous truth: nothing in the stream can violate the predicate.
	fmt.Println(s.All(func(int) bool { return false }))
	fmt.Println(s.Any(func(int) bool { return true }))
	// Output:
	// true
	// false
}
