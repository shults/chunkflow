package chunkflow_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/policy"
	"github.com/shults/chunkflow/seq"
)

func ExampleNew() {
	ctx := context.Background()

	upper, err := chunkflow.New(ctx).Seq(seq.Items("go", "iter", "seq")).
		Map(strings.ToUpper).
		Collect()

	fmt.Println(upper, err)
	// Output: [GO ITER SEQ] <nil>
}

func ExampleBuilder_Chan() {
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

	firstThree, err := chunkflow.New(ctx).Chan(jobs).
		Map(func(i int) int { return i * i }).
		Take(3).
		Collect()

	fmt.Println(firstThree, err)
	// Output: [1 4 9] <nil>
}

func ExampleStream_MapCtx() {
	ctx := context.Background()

	parse := func(_ context.Context, s string) (int, error) {
		var n int
		_, err := fmt.Sscanf(s, "%d", &n)
		return n, err
	}

	// The first error stops the pipeline; values collected before it are returned.
	nums, err := chunkflow.New(ctx).Seq(seq.Items("1", "2", "x", "4")).
		MapCtx(parse).
		Collect()

	fmt.Println(nums, err != nil)
	// Output: [1 2] true
}

func ExampleStream_MapCtx_parallel() {
	ctx := context.Background()

	square := func(_ context.Context, i int) (int, error) { return i * i, nil }

	// Four workers, results still in source order: the parallel step is a drop-in for the
	// sequential one.
	squares, err := chunkflow.New(ctx).Seq(seq.RangeInclusive(1, 6)).
		MapCtx(square, chunkflow.WithParallel(4)).
		Collect()

	fmt.Println(squares, err)
	// Output: [1 4 9 16 25 36] <nil>
}

func ExampleWithUnordered() {
	ctx := context.Background()

	// When only the total matters, WithUnordered lets fast items overtake slow ones instead
	// of waiting for them; the fold below is order-independent, so nothing is lost.
	square := func(_ context.Context, i int) (int, error) { return i * i, nil }
	sum, err := chunkflow.New(ctx).Seq(seq.RangeInclusive(1, 100)).
		MapCtx(square, chunkflow.WithParallel(4), chunkflow.WithUnordered()).
		Reduce(0, func(acc, item int) int { return acc + item })

	fmt.Println(sum, err)
	// Output: 338350 <nil>
}

func ExampleStream_TapCtx() {
	ctx := context.Background()

	// TapCtx can veto an element by returning an error, e.g. an audit check.
	audit := func(_ context.Context, amount int) error {
		if amount > 100 {
			return fmt.Errorf("amount %d exceeds limit", amount)
		}
		return nil
	}

	got, err := chunkflow.New(ctx).Seq(seq.Items(10, 50, 500, 20)).
		TapCtx(audit).
		Collect()

	fmt.Println(got, err)
	// Output: [10 50] amount 500 exceeds limit
}

func ExampleStream_Chunk() {
	ctx := context.Background()

	// Chunk turns N+1 round trips into N/size batches.
	err := chunkflow.New(ctx).Seq(seq.Range(1, 8)).
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

// ExampleStream_Chunk_deduplication shows the recommended way to drop duplicates on a
// large stream: batch with Chunk, ask the store once per batch, flatten back. The library
// deliberately has no Distinct operator, because a global "seen" set has to live somewhere
// with O(unique) memory, and that somewhere should be the user's choice (a database, a
// key-value store, a Bloom filter, ...), not a hidden map inside the pipeline.
func ExampleStream_Chunk_deduplication() {
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

	err := chunkflow.New(ctx).Seq(seq.Items("a", "b", "a", "c", "b", "d", "a")).
		Chunk[[]string](3).
		MapCtx(insertNew).
		Through(chunkflow.Flatten).
		ForEach(func(id string) { fmt.Println("new:", id) })

	fmt.Println(err)
	// Output:
	// new: a
	// new: b
	// new: c
	// new: d
	// <nil>
}

func ExampleStream_Seq() {
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
	for v, err := range chunkflow.New(ctx).Seq(seq.Range(0, 6)).MapCtx(rejectOdd).Through(policy.CircuitBreaker[int](2)).Seq() {
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

func ExampleBuilder_Seq2() {
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

	got, err := chunkflow.New(ctx).Seq2(rows).Collect()
	fmt.Println(got, err)
	// Output: [alice bob] connection lost
}

func ExampleStream_Through() {
	ctx := context.Background()

	// Reusable pipeline fragments are plain functions; Through plugs them in.
	onlyEven := func(s chunkflow.Stream[int]) chunkflow.Stream[int] {
		return s.Filter(func(i int) bool { return i%2 == 0 })
	}

	got, err := chunkflow.New(ctx).Seq(seq.Range(0, 10)).
		Through(onlyEven).
		Chunk[[]int](2).
		Through(chunkflow.Flatten).
		Collect()

	fmt.Println(got, err)
	// Output: [0 2 4 6 8] <nil>
}

func ExampleMerge() {
	ctx := context.Background()

	// Two independent sources (think: two shards) consumed concurrently. The interleaving is
	// not deterministic, so aggregate instead of printing the order.
	shardA := chunkflow.New(ctx).Seq(seq.Range(0, 50))
	shardB := chunkflow.New(ctx).Seq(seq.Range(50, 100))

	n, err := chunkflow.Merge(shardA, shardB).Count()
	fmt.Println(n, err)
	// Output: 100 <nil>
}

func ExampleStream_First() {
	ctx := context.Background()

	// First stops the source after one value; Numbers is infinite.
	v, err := chunkflow.New(ctx).Seq(seq.Numbers(1)).Filter(func(i int) bool { return i%5 == 0 }).First()
	fmt.Println(v, err)

	// An empty stream is not a failure, it is ErrEmpty.
	_, err = chunkflow.New(ctx).Seq(seq.Items[int]()).First()
	fmt.Println(errors.Is(err, chunkflow.ErrEmpty))
	// Output:
	// 5 <nil>
	// true
}

func ExampleSuppress() {
	ctx := context.Background()

	// A malformed record is not a reason to stop the import; the parser says so itself.
	parse := func(_ context.Context, s string) (int, error) {
		n, err := strconv.Atoi(s)
		return n, chunkflow.Suppress(err) // Suppress(nil) is nil
	}

	skipped := 0
	nums, err := chunkflow.New(ctx).Seq(seq.Items("1", "x", "3", "", "5")).
		Opts(chunkflow.WithOnError(func(err error) {
			if errors.Is(err, chunkflow.ErrSuppressed) {
				skipped++
			}
		})).
		MapCtx(parse).
		Collect()

	fmt.Println(nums, err, skipped)
	// Output: [1 3 5] <nil> 2
}

func ExampleStream_ReduceBy() {
	ctx := context.Background()

	words := seq.Items("go", "is", "fun", "and", "go", "is", "fast")

	// Grouping is a fold that appends, starting from nil.
	groups, err := chunkflow.New(ctx).Seq(words).
		ReduceBy(func(w string) int { return len(w) }, nil, func(acc []string, w string) []string {
			return append(acc, w)
		})
	fmt.Println(groups, err)

	// Counting is a fold that adds one, starting from zero.
	counts, err := chunkflow.New(ctx).Seq(words).
		ReduceBy(func(w string) string { return w }, 0, func(n int, _ string) int { return n + 1 })
	fmt.Println(counts, err)
	// Output:
	// map[2:[go is go is] 3:[fun and] 4:[fast]] <nil>
	// map[and:1 fast:1 fun:1 go:2 is:2] <nil>
}

func ExampleStream_First_find() {
	ctx := context.Background()

	// "Find the first match" is Filter followed by First; the source stops right there.
	pulled := 0
	v, err := chunkflow.New(ctx).Seq(seq.Numbers(1)).
		Tap(func(int) { pulled++ }).
		Filter(func(i int) bool { return i%7 == 0 }).
		First()

	fmt.Println(v, err, pulled)
	// Output: 7 <nil> 7
}

func ExampleStream_Zip() {
	ctx := context.Background()

	ids := chunkflow.New(ctx).Seq(seq.Items(7, 8, 9))
	names := chunkflow.New(ctx).Seq(seq.Items("ann", "bob")) // shorter: the pipeline ends with it

	type user struct {
		ID   int
		Name string
	}
	users, err := ids.Zip(names, func(id int, name string) user { return user{ID: id, Name: name} }).Collect()
	fmt.Println(users, err)
	// Output: [{7 ann} {8 bob}] <nil>
}

func ExampleStream_ChunkTimeout() {
	ctx := context.Background()

	// Events trickle in over a channel: three quickly, then a pause, then two more.
	events := make(chan string)
	go func() {
		defer close(events)
		for _, e := range []string{"a", "b", "c"} {
			events <- e
		}
		time.Sleep(300 * time.Millisecond)
		for _, e := range []string{"d", "e"} {
			events <- e
		}
	}()

	// Batches of up to 10, but no event waits longer than 30ms for its batch: the first
	// three go out together long before the pause is over.
	batches, err := chunkflow.New(ctx).Chan(events).ChunkTimeout[[]string](10, 30*time.Millisecond).Collect()
	fmt.Println(batches, err)
	// Output: [[a b c] [d e]] <nil>
}

func ExampleErrPanic() {
	ctx := context.Background()

	var cfg map[string]int // nil map: writing to it panics
	got, err := chunkflow.New(ctx).Seq(seq.Items("a", "b", "c")).
		MapCtx(func(_ context.Context, k string) (int, error) {
			if k == "b" {
				cfg[k] = 1 // bug
			}
			return len(k), nil
		}, chunkflow.WithParallel(2)).
		Collect()

	// The worker's panic did not crash the process; it came back as an error.
	fmt.Println(len(got) < 3, errors.Is(err, chunkflow.ErrPanic))
	// Output: true true
}
