package seq_test

import (
	"fmt"
	"slices"

	"github.com/shults/chunkflow/seq"
)

func ExampleRange() {
	fmt.Println(slices.Collect(seq.Range(0, 5)))
	fmt.Println(slices.Collect(seq.RangeInclusive(0, 5)))
	// Output:
	// [0 1 2 3 4]
	// [0 1 2 3 4 5]
}

func ExampleNumbers() {
	// Infinite: break out of the loop yourself.
	for n := range seq.Numbers(10) {
		if n > 12 {
			break
		}
		fmt.Println(n)
	}
	// Output:
	// 10
	// 11
	// 12
}
