# ChunkFlow

[![Go Reference](https://pkg.go.dev/badge/github.com/shults/chunkflow.svg)](https://pkg.go.dev/github.com/shults/chunkflow)
[![CI](https://github.com/shults/chunkflow/actions/workflows/ci.yml/badge.svg)](https://github.com/shults/chunkflow/actions/workflows/ci.yml)

ChunkFlow is a stream-processing library for Go built on the language's native iterators. It
takes the part of the job that every I/O-bound pipeline reinvents: a context that reaches every
step, errors that travel with the data instead of aborting it, worker pools that keep the input
order, batching with a size and a time bound, retries and circuit breaking. Pipelines are typed
end to end and read left to right, because they are built on Go 1.27 generic methods.

```go
err := chunkflow.New(ctx).Chan(events).                   // any iter.Seq, iter.Seq2, (T, error) iterator or channel
    MapCtx(enrich, chunkflow.WithParallel(8)).            // I/O on 8 workers, results in source order
    Through(policy.CircuitBreaker[Enriched](5)).          // tolerate a flaky dependency, trip on 5 in a row
    ChunkTimeout[[]Enriched](100, 30*time.Millisecond).   // batches of 100, or whatever arrived in 30ms
    ForEachCtx(store)                                     // one write per batch
```

## Goals

- **One type.** `Stream[T]` carries a `context.Context` and lets errors flow as elements. There
  is no second, simpler stream for in-memory work; pass `context.Background()` and ignore the
  error you know cannot happen.
- **Errors are data, bugs are not.** An error from a source or a callback moves down the pipeline
  like a value. Operators pass it along, terminals stop at the first one, and it is tolerated only
  when something says so: a policy, or the callback itself with `Suppress`. Nothing is dropped or
  logged behind your back. A panic in a callback becomes `ErrPanic`, which nothing may tolerate.
- **Parallel without losing order.** `WithParallel(n)` runs a step on `n` workers and emits in
  source order behind a bounded window, so a parallel step is a drop-in for the sequential one.
  `WithUnordered()` trades that for throughput when order does not matter.
- **Bounded by construction.** Nothing runs until a terminal pulls, and nothing is buffered
  except what you asked for: a chunk's size, a pool's window. Allocations are constant in the
  stream's length; see [BENCHMARK.md](BENCHMARK.md) for the numbers.
- **Native in, native out.** Sources are `iter.Seq`, `iter.Seq2` (as `Entry` key/value pairs),
  `iter.Seq2[T, error]` or channels; `Seq()` hands the pipeline back as an iterator. Everything that produces or consumes Go iterators
  already works with it.
- **Extensible on one seam.** `Transform` exposes the raw `(value, error)` sequence and `Suppress`
  marks an error as tolerated. The `policy` and `step` sub-packages are written on that and nothing
  else, so an extension in your module has the same tools.

Requires Go 1.27 or newer.

```bash
go get -u github.com/shults/chunkflow
```

## Quick start

```go
package main

import (
    "context"
    "errors"
    "fmt"

    "github.com/shults/chunkflow"
    "github.com/shults/chunkflow/policy"
    "github.com/shults/chunkflow/seq"
)

func main() {
    ctx := context.Background()

    // Squares of the first even numbers, in batches of 3, flattened back. Numbers is infinite;
    // Take decides how much of it is ever generated.
    res, err := chunkflow.New(ctx).Seq(seq.Numbers(1)).
        Filter(func(i int) bool { return i%2 == 0 }).
        Map(func(i int) int { return i * i }).
        Take(7).
        Chunk[[]int](3).
        Through(chunkflow.Flatten). // Flatten changes the element type, so it is a function
        Collect()
    fmt.Println(res, err) // [4 16 36 64 100 144 196] <nil>

    // A fallible step. The first error ends the pipeline; values collected so far are returned.
    parse := func(_ context.Context, s string) (int, error) {
        var n int
        _, err := fmt.Sscanf(s, "%d", &n)
        return n, err
    }
    nums, err := chunkflow.New(ctx).Seq(seq.Items("1", "2", "x", "4")).MapCtx(parse).Collect()
    fmt.Println(nums, err != nil) // [1 2] true

    // Tolerating errors: policy.CircuitBreaker(3) lets two failures in a row through and trips on
    // the third. Policies enter through Through; the element type cannot be inferred from the
    // threshold and is spelled out. Tolerated errors stay visible through Seq() or WithOnError.
    tolerated := 0
    nums, err = chunkflow.New(ctx).Seq(seq.Items("1", "x", "3", "y", "5")).
        Opts(chunkflow.WithOnError(func(err error) {
            if errors.Is(err, chunkflow.ErrSuppressed) {
                tolerated++
            }
        })).
        MapCtx(parse).
        Through(policy.CircuitBreaker[int](3)).
        Collect()
    fmt.Println(nums, err, tolerated) // [1 3 5] <nil> 2
}
```

Every operation has a runnable example on [pkg.go.dev](https://pkg.go.dev/github.com/shults/chunkflow);
the examples below are taken from those tests, so they compile and their output is checked.

## Real-time event processing

The shape most pipelines end up with: events are produced all over the application, in bursts,
and have to be written somewhere in batches without making the producers wait for the database.
Wrap the entry point in a struct with `Push` and `Stop`, inject the store, and let the stream
behind it do the work.

```go
type Event struct {
    ID      int
    Payload string
}

// DB is the dependency injected into the saver: anything that stores a batch.
type DB interface {
    Insert(ctx context.Context, events []Event) error
}

// BatchEventSaver collects events and writes them to the DB in batches: a full batch goes out
// at once, a partial one after maxWait. Push never waits for the database, only for room in
// the buffer. A failed insert loses that batch and is reported through the error callback; the
// saver itself keeps running. That is the trade-off of this architecture, chosen on purpose.
type BatchEventSaver struct {
    events  chan Event
    done    chan struct{}
    onError atomic.Pointer[func(error)]
}

func NewBatchEventSaver(ctx context.Context, db DB, size int, maxWait time.Duration) *BatchEventSaver {
    s := &BatchEventSaver{events: make(chan Event, 1024), done: make(chan struct{})}
    go s.run(ctx, db, size, maxWait)
    return s
}

func (s *BatchEventSaver) Push(e Event)             { s.events <- e }          // safe for concurrent use
func (s *BatchEventSaver) Stop()                    { close(s.events); <-s.done } // waits for the last batch
func (s *BatchEventSaver) SetOnError(fn func(error)) { s.onError.Store(&fn) }

func (s *BatchEventSaver) run(ctx context.Context, db DB, size int, maxWait time.Duration) {
    defer close(s.done)
    report := func(err error) {
        if fn := s.onError.Load(); fn != nil {
            (*fn)(err)
        }
    }
    insert := func(ctx context.Context, batch []Event) error {
        return chunkflow.Suppress(db.Insert(ctx, batch)) // a lost batch is reported, not fatal
    }
    err := chunkflow.New(ctx).Chan(s.events).
        Opts(chunkflow.WithOnError(report)).          // every tolerated error reaches the callback
        ChunkTimeout[[]Event](size, maxWait).         // batches of size, or whatever arrived in maxWait
        TapCtx(insert).                               // the write; its error is suppressed above
        Drain()                                       // runs until Stop closes the channel
    if err != nil {
        report(err) // only the context can end this pipeline early
    }
}
```

Three events, a pause longer than `maxWait`, two more, and a database that refuses the second
batch:

```go
saver := NewBatchEventSaver(ctx, db, 100, 30*time.Millisecond)
saver.SetOnError(func(err error) { fmt.Println("lost:", err) })

for _, id := range []int{1, 2, 3} {
    saver.Push(Event{ID: id})
}
time.Sleep(300 * time.Millisecond)
for _, id := range []int{4, 5} {
    saver.Push(Event{ID: id})
}
saver.Stop()
// insert [1 2 3]                            <- released by the 30ms clock, not by the size
// lost: suppressed: insert [4 5]: db down   <- the insert failed; reported, and the saver went on
// stopped
```

Every line of `run` is one decision, and each is replaceable: `Chunk` instead of `ChunkTimeout`
for a source that never pauses; `step.Decorate(insert, step.Retry(3))` before giving up on a
batch; `MapCtx(enrich, WithParallel(8))` before the batching to call a service per event with the
order kept; `policy.CircuitBreaker` to stop hammering a database that is down. The full program
is `Example_batchEventSaver` in `example_event_test.go`, and its output is checked by `go test`.

## Batching

`Chunk(size)` groups values into slices and is free of goroutines: it emits when a chunk is full
or when the source ends. `ChunkTimeout(size, maxWait)` also releases a partial chunk once
`maxWait` has passed since its first value, which is what a trickling source needs. Both keep the
guarantee that a chunk has at least one value, so `batch[0]` is always safe. Errors pass through
both immediately; the partial chunk waits for the next value.

```go
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

batches, err := chunkflow.New(ctx).Chan(events).ChunkTimeout[[]string](10, 30*time.Millisecond).Collect()
fmt.Println(batches, err) // [[a b c] [d e]] <nil>
```

`Flatten` is the inverse (`Chunk(3).Through(chunkflow.Flatten)`), and `Chunk` before a `MapCtx`
that takes a slice is how you turn per-item I/O into bulk I/O.

## Errors: fail fast, tolerate on purpose

A pipeline without a policy is fail-fast: the first error stops the terminal and the source is not
read past the failing element. Three things change that, each explicit.

**A callback that knows better** returns `Suppress(err)`. The element is skipped, the pipeline
continues, and the error is still visible to `WithOnError` and `Seq()`:

```go
parse := func(_ context.Context, s string) (int, error) {
    n, err := strconv.Atoi(s)
    return n, chunkflow.Suppress(err) // Suppress(nil) is nil; a bad record is not a reason to stop
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
fmt.Println(nums, err, skipped) // [1 3 5] <nil> 2
```

**A policy on the stream** decides for a whole stage. `policy.CircuitBreaker(n)` tolerates up to
`n-1` consecutive errors, marking them `ErrSuppressed`, and trips on the n-th with a fatal error:

```go
got, err := chunkflow.New(ctx).Seq(seq.Range(1, 7)).
    MapCtx(fetch).                                // fails for 3 and 4
    Through(policy.CircuitBreaker[int](3)).       // two in a row are tolerated, a third would trip
    Collect()
fmt.Println(got, err) // [10 20 50 60] <nil>
```

**A decorator on the call** wraps the callback you do not own: `step.Tolerate(errNotFound)`
suppresses a specific error, `step.Retry` repeats a failure. See [Retries and timeouts](#retries-and-timeouts-per-call).

Whatever tolerated an error, `Seq()` shows it, and the switch below is the whole vocabulary:

```go
for v, err := range stream.Through(policy.CircuitBreaker[User](5)).Seq() {
    switch {
    case err == nil:
        use(v)
    case errors.Is(err, chunkflow.ErrSuppressed):
        metrics.Inc("tolerated")  // the original error is still in the chain
    case errors.Is(err, chunkflow.ErrPanic):
        log.Fatal(err)            // a bug in a callback; message has the value and the stack
    default:
        return err                // breaker tripped, context cancelled, source failed, ...
    }
}
```

- Operators work on **values**: `Take(3)` takes three values however many errors pass by, `Chunk`
  never puts an error into a slice, `Compact` compares only values.
- A pipeline without a policy is fail-fast: the source is not consumed past the failing element.
- A callback that knows an error is tolerable returns `Suppress(err)`: the element is replaced by the
  marked error, terminals skip it, `WithOnError` and `Seq()` still see it, `policy.CircuitBreaker`
  does not count it. `Suppress(nil)` is `nil`; context errors and `ErrPanic` cannot be suppressed and come
  back unchanged.
- Cancelling the context always surfaces as an error, also when a worker pool exits without emitting.
- `First` and `Last` on a stream without values return `ErrEmpty`. It is a plain sentinel for "nothing
  to return", not a failure: it never reaches `WithOnError`, and `errors.Is(err, ErrEmpty)` tells it
  apart from the errors above.

## Parallelism and order

`WithParallel(n)` on `MapCtx`, `FilterCtx` or `TapCtx` runs the callback on `n` workers. Results
come out in source order, behind a read-ahead window of `2n` elements, so the parallel stage is
a drop-in for the sequential one:

```go
square := func(_ context.Context, i int) (int, error) { return i * i, nil }

squares, err := chunkflow.New(ctx).Seq(seq.RangeInclusive(1, 6)).
    MapCtx(square, chunkflow.WithParallel(4)).
    Collect()
fmt.Println(squares, err) // [1 4 9 16 25 36] <nil>
```

Ordering has a price: one slow element holds back its successors, and once the window is full
the workers wait. When the consumer does not care, `WithUnordered()` emits results as they
finish, which is the right call for side-effect pipelines ending in `Drain`:

```go
sum, err := chunkflow.New(ctx).Seq(seq.RangeInclusive(1, 100)).
    MapCtx(square, chunkflow.WithParallel(4), chunkflow.WithUnordered()).
    Reduce(0, func(acc, item int) int { return acc + item })
fmt.Println(sum, err) // 338350 <nil>
```

A pool costs about half a microsecond per element in handoffs, so it pays once the callback
costs more than a few microseconds, which every network or disk call does; below that, `Map` on
one goroutine is faster. Eight workers on 200 µs calls give a 7.9× speed-up with order kept,
within 1% of unordered. Numbers in [BENCHMARK.md](BENCHMARK.md).

## Retries and timeouts per call

Policies see the stream; the `step` package sees the individual call. A `step.Middleware` wraps
one invocation and knows only its context and its error, so one set of them serves every
callback shape: `Decorate` for `func(ctx, T) (R, error)`, `Decorate3` for fold callbacks. The first
middleware in the list is the outermost.

```go
fetch := func(_ context.Context, id int) (string, error) { /* flaky */ }

users, err := chunkflow.New(ctx).Seq(seq.Items(1, 2)).
    MapCtx(step.Decorate(fetch,
        step.Retry(3),                  // up to three attempts...
        step.Timeout(time.Second),      // ...each with its own one-second deadline
    )).
    Collect()
```

`Retry` never repeats a call whose caller context is done or whose error was marked by `Suppress`,
and a deadline set by an inner `Timeout` is not the caller's context, so a timed-out attempt is
retried. Pacing is injected, not built in: `step.RetryWithBackoff(newBackoff)` takes a constructor
for anything with the `NextBackOff() / Reset()` method set of `cenkalti/backoff`, one fresh
instance per element, no dependency taken. Retries and a breaker compose: three attempts per
element, then a breaker over five failed elements in a row.

## Joining streams

`Zip(other, fn)` pairs two streams position by position through a callback and ends at the
shorter side; a third source is another `Zip` whose callback extends your struct, so fields keep
their names. `Concat` runs streams one after another, `Merge` concurrently as they arrive.

```go
ids := chunkflow.New(ctx).Seq(seq.Items(7, 8, 9))
names := chunkflow.New(ctx).Seq(seq.Items("ann", "bob")) // shorter: the pipeline ends with it

type user struct {
    ID   int
    Name string
}
users, err := ids.Zip(names, func(id int, name string) user { return user{ID: id, Name: name} }).Collect()
fmt.Println(users, err) // [{7 ann} {8 bob}] <nil>
```

## Iterators in and out

`New(ctx).Seq(...)` wraps any `iter.Seq[T]`; `Seq2` any two-value iterator such as `maps.All` or
`slices.All`, as a stream of `Entry{Key, Value}`; `SeqErr` an `iter.Seq2[T, error]` such as a
database cursor, whose errors become the stream's errors; `Chan` a receive channel. `Seq()` on a stream returns `iter.Seq2[T, error]` for a plain
`range` loop, and `Transform` lets a function rewrite that raw sequence and hand it back as a
stream with the same context and options. The `seq` sub-package has the usual generators
(`Items`, `Range`, `Numbers`, `Repeat`, `Iterate`) as plain `iter.Seq`, so they work with
`slices.Collect` and `range` as well as here.

## API

### Building a stream

`New` binds the context; the source method picks the element type. Options come afterwards,
through `Opts` on the stream or on the individual `*Ctx` call.

| | |
| --- | --- |
| `New(ctx)` | Starts a `Stream` bound to `ctx`. |
| `.Seq(iter.Seq[T])` | Wraps a native iterator. The context is checked before every element. |
| `.Seq2(iter.Seq2[K, V])` | Wraps a two-value iterator (`maps.All`, `slices.All`) as `Stream[Entry[K, V]]`. |
| `.SeqErr(iter.Seq2[T, error])` | Wraps a `(value, error)` iterator, the inverse of `Stream.Seq()`; the errors become the stream's errors. |
| `.Chan(<-chan T)` | Reads a channel until it is closed or the context is cancelled. Single-use; stopping early does not close or drain the channel. |

Generators in `github.com/shults/chunkflow/seq` return plain `iter.Seq[T]`:

| | | |
| --- | --- | --- |
| `seq.Items(...T)` | the arguments | finite |
| `seq.Range(from, to)` | integers in `[from, to)`, any integer type | finite |
| `seq.RangeInclusive(from, to)` | integers in `[from, to]`, safe at `MaxInt` | finite |
| `seq.Repeat(val, n)` | `val` exactly `n` times | finite |
| `seq.Numbers(start)` | `start`, `start+1`, ... | **infinite** |
| `seq.Const(val)` | `val` forever | **infinite** |
| `seq.Iterate(seed, fn)` | `seed`, `fn(seed)`, `fn(fn(seed))`, ... | **infinite** |

Anything else that yields `iter.Seq` plugs in directly: `slices.Values`, `maps.Keys`,
`strings.Lines`, a database cursor wrapped as `iter.Seq2[Row, error]`.

### Intermediate operations

Lazy; nothing runs until a terminal pulls. Each `*Ctx` variant takes a callback that receives the
`context.Context` and may return an error. The plain variant is the `*Ctx` one on a single worker.

| Method | Signature | Notes |
| --- | --- | --- |
| `Map` / `MapCtx` | `Map[R](func(T) R)` · `MapCtx[R](func(ctx, T) (R, error), ...StepOption)` | `WithParallel(n)` runs the callback on `n` workers, results in source order; `WithUnordered()` emits them as they arrive. |
| `Filter` / `FilterCtx` | `Filter(func(T) bool)` · `FilterCtx(func(ctx, T) (bool, error), ...StepOption)` | Same concurrency semantics as `MapCtx`. |
| `Tap` / `TapCtx` | `Tap(func(T))` · `TapCtx(func(ctx, T) error, ...StepOption)` | Side effect, element passes through; an error from `TapCtx` replaces the element. |
| `Take` / `Skip` | `Take(n)` · `Skip(n)` | Count values, not errors. `Take` stops pulling from the source. |
| `TakeWhile` / `SkipWhile` | `TakeWhile(func(T) bool)` · `SkipWhile(func(T) bool)`, `*Ctx` variants | Stop / start emitting at the first `false`; `SkipWhile` stops evaluating afterwards. |
| `CompactFunc` | `CompactFunc(func(a, b T) bool)` | Drops **consecutive** duplicates in O(1) memory; input must be sorted or grouped for a global dedup. |
| `Chunk` | `Chunk[R []T](size)` | Groups values into slices of `size`; the last one may be shorter. Errors pass through, the partial chunk is kept. |
| `ChunkTimeout` | `ChunkTimeout[R []T](size, maxWait)` | `Chunk` that also releases a partial chunk once `maxWait` has passed since its first value: batching for sources that trickle. Reads the source on a goroutine, so it costs a channel handoff per value. |
| `Zip` / `ZipCtx` | `Zip[O, R](other Stream[O], func(T, O) R)` · `ZipCtx[O, R](other, func(ctx, T, O) (R, error))` | Pairs values position by position through the callback, ends at the shorter side. Errors pass at their position without consuming a value on the other side. Context merged like `Merge`. A third source is another `Zip` extending your struct. |
| `Through` | `Through[R](func(Stream[T]) Stream[R])` | Plugs a top-level function into the chain, keeping left-to-right order. |
| `Opts` | `Opts(...Option)` | Pipeline options for everything downstream. |

### Top-level functions

Operations that change the element type or need a constraint a method cannot express. Use them
with `.Through()`.

| | |
| --- | --- |
| `Flatten[E](Stream[[]E]) Stream[E]` | Unwraps a stream of slices. |
| `Compact[T comparable](Stream[T])` | `CompactFunc` with `==`. |
| `Concat(...Stream[T])` | One stream after another, deterministic; errors keep their position. |
| `Merge(...Stream[T])` | All streams concurrently, interleaved as they arrive; cancelled by any source's context, with the original cause kept. |
| `Suppress(err) error` | Marks an error as tolerated, inside a `*Ctx` callback or a policy (see below). |

### Policies and extensions

Error policies are not operations on values, so they are functions for `.Through()` in the
`policy` sub-package. The element type cannot be inferred from a policy's arguments and is
spelled out.

| | |
| --- | --- |
| `policy.CircuitBreaker[T](n)` | Tolerates up to `n-1` errors in a row by re-emitting them marked `ErrSuppressed`; trips on the `n`-th. Never tolerates context errors or `ErrPanic`. |

Step decorators in the `step` sub-package wrap the callback itself instead of the stream, so they
see the individual call and can repeat or bound it, which a policy cannot. A `step.Middleware`
sees only the context and the error of one call, values stay in a closure, so one set of them
serves every callback shape without type arguments. `Decorate` applies them to a
`func(ctx, T) (R, error)`, `Decorate3` to a fold callback `func(ctx, Acc, T) (Acc, error)`; the
first middleware in the list is the outermost.

| | |
| --- | --- |
| `step.Retry(attempts, opts...)` | Repeats a failed call up to `attempts` times, immediately or paced by `step.RetryWithBackoff(newBackoff)`, where the constructor returns anything with the `NextBackOff() / Reset()` method set of `cenkalti/backoff`, created fresh per element. The package ships no pacing of its own and takes no dependency. Never past a done caller context or an error marked by `Suppress`; a timeout from an inner `step.Timeout` is retried. |
| `step.Timeout(d)` | Runs the call with a context that expires after `d`. Cooperative: the callback must honour its context. |
| `step.Tolerate(targets...)` | `Suppress` for callbacks you do not own: an error matching any target skips the element. In `Decorate` that is the marked error; in `Decorate3` the fold keeps its accumulator and carries on. |

```go
users.MapCtx(step.Decorate(fetch, step.Retry(3, step.RetryWithBackoff(newBackoff)), step.Timeout(2*time.Second)),
        chunkflow.WithParallel(8)).
    Through(policy.CircuitBreaker[User](5))   // 3 attempts of 2s per element, then a breaker over 5 elements in a row
```

Writing your own: `Stream.Transform[R](func(iter.Seq2[T, error]) iter.Seq2[R, error]) Stream[R]`
gives a function the raw element sequence, fatal errors included and without stopping at them,
and wraps the result back into a stream with the same context and options. Mark what you tolerate
with `Suppress`. `policy.CircuitBreaker` uses nothing else.

### Terminal operations

Every terminal returns an `error`: the first non-suppressed error stops consumption and is returned,
together with whatever was produced so far.

| Method | Returns | Notes |
| --- | --- | --- |
| `Collect()` | `([]T, error)` | Everything into a slice. Do not use on infinite streams. |
| `ForEach` / `ForEachCtx` | `error` | Side effect per element. |
| `Reduce[R](init R, func(acc R, item T) R)` / `ReduceCtx` | `(R, error)` | Fold into an accumulator of any type, `(acc, item)` order, `init` first. Always sequential. |
| `ReduceBy[K, R](key func(T) K, init R, func(acc R, item T) R)` / `ReduceByCtx` | `(map[K]R, error)` | One fold per key: group (`nil`, append), count (`0`, `+1`), sum, max. `init` is copied per key, so start from a scalar or `nil`. Materialises like `Collect`. |
| `Count()` | `(int, error)` | |
| `Drain()` | `error` | Exhausts the stream, discards values; the terminal for side-effect pipelines. |
| `All` / `AllCtx` | `(bool, error)` | Short-circuits on the first mismatch. **Empty stream: `true`** (vacuous truth). |
| `Any` / `AnyCtx` | `(bool, error)` | Short-circuits on the first match. **Empty stream: `false`.** |
| `First()` | `(T, error)` | Stops the source after one value. **Empty stream: `ErrEmpty`.** |
| `Last()` | `(T, error)` | **Empty stream: `ErrEmpty`.** |
| `Seq()` | `iter.Seq2[T, error]` | The pipeline as a native iterator, suppressed errors included; stops after the first fatal one. |

### Options

Two kinds, checked by the compiler:

- `StepOption` configures one `*Ctx` call: `WithParallel(n)` and `WithUnordered()`. Passing it to
  `Opts` makes it the default for everything downstream.
  - `WithParallel(n)` keeps source order. The step reads ahead of the slowest item by a window of
    `2n` items; when the window is full, idle workers wait instead of pulling, so memory stays
    bounded and a single slow item stalls the step behind it (head-of-line blocking).
  - `WithUnordered()` drops the ordering and the window: results leave as workers finish them, and a
    downstream `policy.CircuitBreaker` counts consecutive errors in arrival order.
- `Option` configures the whole pipeline only: `WithOnError(fn)` registers the hook every terminal
  calls for each error it handles, suppressed ones just before skipping them, the fatal one just
  before returning it. It is the place for logging and metrics.

Invalid values (`WithParallel(0)`, `WithOnError(nil)`) do not panic: the stream they are applied to
emits a single error and ends.

## Performance

Measured, not claimed: [BENCHMARK.md](BENCHMARK.md). The short version: a sequential stage costs
about 10 ns per element and a constant handful of allocations per stream, a parallel stage about
half a microsecond per element in channel handoffs, so `WithParallel` pays once the callback
costs more than a few microseconds, which every I/O call does. Eight workers on 200 µs calls give
a 7.9× speed-up with source order kept, within 1% of `WithUnordered()`.

## Versioning

Pre-1.0. A patch release changes no exported API; a minor release adds API or breaks it, and every
break is listed in [CHANGELOG.md](CHANGELOG.md) with the migration. `v0.1.0` freezes the core API:
from there on a break is a deliberate minor bump with a migration note, not a routine.

## Development

```bash
make setup   # installs golangci-lint and govulncheck into ./bin, enables the git pre-commit hook
make check   # gofmt, go vet, golangci-lint, go mod tidy -diff — what the hook runs
make test    # go test -race -shuffle=on
make ci      # check + test + govulncheck, mirrors the GitHub Actions pipeline
make bench   # every benchmark 8 times into bench.out, summarised with benchstat
```

Design decisions and their reasons are in [CONVENTIONS.md](CONVENTIONS.md), the plan in
[ROADMAP.md](ROADMAP.md), contributor instructions in [AGENTS.md](AGENTS.md).
