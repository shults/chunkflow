package chunkflow_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
)

func ExampleNewIo() {
	ctx := context.Background()

	upper, err := chunkflow.NewIo(ctx).Seq(seq.Items("go", "iter", "seq")).
		Map(strings.ToUpper).
		Collect()

	fmt.Println(upper, err)
	// Output: [GO ITER SEQ] <nil>
}

func ExampleIoBuilder_Chan() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A producer that respects the same context: if the consumer stops early the
	// producer is released too, instead of blocking forever on a send.
	jobs := make(chan int)
	go func() {
		defer close(jobs)
		for i := 1; ; i++ {
			select {
			case jobs <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	firstThree, err := chunkflow.NewIo(ctx).Chan(jobs).
		Map(func(i int) int { return i * i }).
		Take(3).
		Collect()

	fmt.Println(firstThree, err)
	// Output: [1 4 9] <nil>
}

func ExampleIoStream_MapCtx() {
	ctx := context.Background()

	parse := func(_ context.Context, s string) (int, error) {
		var n int
		_, err := fmt.Sscanf(s, "%d", &n)
		return n, err
	}

	// The first error stops the pipeline; values collected before it are returned.
	nums, err := chunkflow.NewIo(ctx).Seq(seq.Items("1", "2", "x", "4")).
		MapCtx(parse).
		Collect()

	fmt.Println(nums, err != nil)
	// Output: [1 2] true
}

func ExampleIoStream_MapCtx_parallel() {
	ctx := context.Background()

	square := func(_ context.Context, i int) (int, error) { return i * i, nil }

	// With WithParallel(n) the output order is not guaranteed, so reduce instead of collecting.
	sum, err := chunkflow.NewIo(ctx).Seq(seq.RangeInclusive(1, 100)).
		MapCtx(square, chunkflow.WithParallel(4)).
		Reduce(0, func(acc, item int) int { return acc + item })

	fmt.Println(sum, err)
	// Output: 338350 <nil>
}

func ExampleIoStream_TapCtx() {
	ctx := context.Background()

	// TapCtx can veto an element by returning an error, e.g. an audit check.
	audit := func(_ context.Context, amount int) error {
		if amount > 100 {
			return fmt.Errorf("amount %d exceeds limit", amount)
		}
		return nil
	}

	got, err := chunkflow.NewIo(ctx).Seq(seq.Items(10, 50, 500, 20)).
		TapCtx(audit).
		Collect()

	fmt.Println(got, err)
	// Output: [10 50] amount 500 exceeds limit
}

func ExampleIoStream_Chunk() {
	ctx := context.Background()

	// Chunk turns N+1 round trips into N/size batches.
	err := chunkflow.NewIo(ctx).Seq(seq.Range(1, 8)).
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

// ExampleIoStream_Chunk_deduplication shows the recommended way to drop duplicates on a
// large stream: batch with Chunk, ask the store once per batch, flatten back. The library
// deliberately has no Distinct operator, because a global "seen" set has to live somewhere
// with O(unique) memory, and that somewhere should be the user's choice (a database, a
// key-value store, a Bloom filter, ...), not a hidden map inside the pipeline.
func ExampleIoStream_Chunk_deduplication() {
	ctx := context.Background()

	// Stand-in for an external store, e.g. an `INSERT ... ON CONFLICT DO NOTHING RETURNING id`.
	store := map[string]struct{}{}
	insertNew := func(_ context.Context, batch []string) ([]string, error) {
		var fresh []string
		for _, id := range batch {
			if _, dup := store[id]; dup {
				continue
			}
			store[id] = struct{}{}
			fresh = append(fresh, id)
		}
		return fresh, nil // one round trip per batch, not per element
	}

	err := chunkflow.NewIo(ctx).Seq(seq.Items("a", "b", "a", "c", "b", "d", "a")).
		Chunk[[]string](3).
		MapCtx(insertNew).
		Through(chunkflow.IoFlatten).
		ForEach(func(id string) { fmt.Println("new:", id) })

	fmt.Println(err)
	// Output:
	// new: a
	// new: b
	// new: c
	// new: d
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
	got, err := chunkflow.NewIo(ctx).Seq(seq.Range(1, 7)).
		MapCtx(fetch).
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

	got, err := chunkflow.NewIo(ctx).Seq(seq.Numbers(1)). // infinite source
								MapCtx(fetch).
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
	for v, err := range chunkflow.NewIo(ctx).Seq(seq.Range(0, 6)).MapCtx(rejectOdd).CircuitBreaker(2).Seq() {
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

func ExampleIoBuilder_Seq2() {
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

	got, err := chunkflow.NewIo(ctx).Seq2(rows).Collect()
	fmt.Println(got, err)
	// Output: [alice bob] connection lost
}

func ExampleIoStream_Through() {
	ctx := context.Background()

	// Reusable pipeline fragments are plain functions; Through plugs them in.
	onlyEven := func(s chunkflow.IoStream[int]) chunkflow.IoStream[int] {
		return s.Filter(func(i int) bool { return i%2 == 0 })
	}

	got, err := chunkflow.NewIo(ctx).Seq(seq.Range(0, 10)).
		Through(onlyEven).
		Chunk[[]int](2).
		Through(chunkflow.IoFlatten).
		Collect()

	fmt.Println(got, err)
	// Output: [0 2 4 6 8] <nil>
}

func ExampleIoMerge() {
	ctx := context.Background()

	// Two independent sources (think: two shards) consumed concurrently. The interleaving is
	// not deterministic, so aggregate instead of printing the order.
	shardA := chunkflow.NewIo(ctx).Seq(seq.Range(0, 50))
	shardB := chunkflow.NewIo(ctx).Seq(seq.Range(50, 100))

	n, err := chunkflow.IoMerge(shardA, shardB).Count()
	fmt.Println(n, err)
	// Output: 100 <nil>
}
