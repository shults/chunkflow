package chunkflow_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
)

func ExampleNewIoStream() {
	ctx := context.Background()

	upper, err := chunkflow.NewIoStream(ctx, seq.Items("go", "iter", "seq")).
		Map(strings.ToUpper).
		Collect()

	fmt.Println(upper, err)
	// Output: [GO ITER SEQ] <nil>
}

func ExampleIoStream_MapAsync() {
	ctx := context.Background()

	parse := func(_ context.Context, s string) (int, error) {
		var n int
		_, err := fmt.Sscanf(s, "%d", &n)
		return n, err
	}

	// The first error stops the pipeline; values collected before it are returned.
	nums, err := chunkflow.NewIoStream(ctx, seq.Items("1", "2", "x", "4")).
		MapAsync(parse).
		Collect()

	fmt.Println(nums, err != nil)
	// Output: [1 2] true
}

func ExampleIoStream_MapAsync_parallel() {
	ctx := context.Background()

	square := func(_ context.Context, i int) (int, error) { return i * i, nil }

	// With WithParallel(n) the output order is not guaranteed, so reduce instead of collecting.
	sum, err := chunkflow.NewIoStream(ctx, seq.RangeInclusive(1, 100)).
		MapAsync(square, chunkflow.WithParallel(4)).
		Reduce(0, func(item, acc int) int { return acc + item })

	fmt.Println(sum, err)
	// Output: 338350 <nil>
}

func ExampleIoStream_Chunk() {
	ctx := context.Background()

	// Chunk turns N+1 round trips into N/size batches.
	err := chunkflow.NewIoStream(ctx, seq.Range(1, 8)).
		Chunk(3).
		ForEach(func(batch []int) {
			fmt.Println("insert", batch)
		})

	fmt.Println(err)
	// Output:
	// insert [1 2 3]
	// insert [4 5 6]
	// insert [7]
	// <nil>
}

func ExampleIoStream_CircuitBreaker() {
	ctx := context.Background()
	errFlaky := errors.New("flaky")

	fetch := func(_ context.Context, i int) (int, error) {
		if i == 3 || i == 4 {
			return 0, errFlaky
		}
		return i * 10, nil
	}

	// Up to 2 consecutive failures are tolerated; the 3rd in a row would trip the breaker.
	got, err := chunkflow.NewIoStream(ctx, seq.Range(1, 7)).
		MapAsync(fetch).
		CircuitBreaker(3).
		Collect()

	fmt.Println(got, err)
	// Output: [10 20 50 60] <nil>
}

func ExampleIoStream_CircuitBreaker_tripped() {
	ctx := context.Background()
	errDown := errors.New("service down")

	fetch := func(_ context.Context, i int) (int, error) {
		if i >= 3 {
			return 0, errDown
		}
		return i, nil
	}

	got, err := chunkflow.NewIoStream(ctx, seq.Numbers(1)). // infinite source
								MapAsync(fetch).
								CircuitBreaker(3).
								Collect()

	fmt.Println(got)
	fmt.Println(err)
	fmt.Println(errors.Is(err, errDown))
	// Output:
	// [1 2]
	// circuit breaker tripped after 3 consecutive errors: service down
	// true
}

func ExampleIoStream_Seq() {
	ctx := context.Background()
	errOdd := errors.New("odd")

	rejectOdd := func(_ context.Context, i int) (int, error) {
		if i%2 == 1 {
			return 0, errOdd
		}
		return i, nil
	}

	// Seq exposes suppressed errors instead of hiding them, so the consumer can
	// count or log what the breaker tolerated.
	tolerated := 0
	for v, err := range chunkflow.NewIoStream(ctx, seq.Range(0, 6)).MapAsync(rejectOdd).CircuitBreaker(2).Seq() {
		switch {
		case err == nil:
			fmt.Println("value", v)
		case errors.Is(err, chunkflow.ErrSuppressed):
			tolerated++
		default:
			fmt.Println("fatal", err)
		}
	}
	fmt.Println("tolerated", tolerated)
	// Output:
	// value 0
	// value 2
	// value 4
	// tolerated 3
}

func ExampleNewIoStream2() {
	ctx := context.Background()

	// A (value, error) iterator, e.g. wrapping a database cursor.
	rows := func(yield func(string, error) bool) {
		for _, r := range []string{"alice", "bob"} {
			if !yield(r, nil) {
				return
			}
		}
		yield("", errors.New("connection lost"))
	}

	got, err := chunkflow.NewIoStream2(ctx, rows).Collect()
	fmt.Println(got, err)
	// Output: [alice bob] connection lost
}

func ExampleIoStream_Through() {
	ctx := context.Background()

	// Reusable pipeline fragments are plain functions; Through plugs them in.
	onlyEven := func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] {
		return s.Filter(func(i int) bool { return i%2 == 0 })
	}

	got, err := chunkflow.NewIoStream(ctx, seq.Range(0, 10)).
		Through(onlyEven).
		Chunk[[]int](2).
		Through(chunkflow.IoFlatten).
		Collect()

	fmt.Println(got, err)
	// Output: [0 2 4 6 8] <nil>
}
