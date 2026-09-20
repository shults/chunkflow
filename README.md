# ChunkFlow

Lazy, type-safe data pipelines on Go's native iterators, with what I/O-bound stream processing
needs on top: a context, errors that travel as elements, worker pools, a circuit breaker and
chunking. Built on Go 1.27 generic methods, so the whole pipeline reads left to right.

```go
err := chunkflow.New(ctx).Chan(userIDs).                 // any iter.Seq, iter.Seq2 or channel
    MapCtx(fetchUser, chunkflow.WithParallel(8)).        // I/O on 8 workers
    Through(policy.CircuitBreaker[User](5)).             // tolerate flaky lookups, trip on 5 in a row
    Filter(func(u User) bool { return u.Active }).
    Chunk[[]User](500).                                  // batch for the database
    ForEachCtx(insertBatch)                              // one INSERT per 500 users
```

## Why

- **One type.** `Stream[T]` carries a `context.Context` and an error channel. Pure in-memory work
  passes `context.Background()` and ignores the error it knows cannot happen.
- **Lazy and bounded.** Every intermediate operation is a closure over the upstream iterator;
  nothing runs until a terminal pulls, and nothing is buffered except by `Chunk` (bounded by its
  size) and by worker pools (bounded by twice `WithParallel`). `Take`, `First`, `Any` and `All`
  stop the source as soon as they can.
- **Parallel, still in order.** `WithParallel(n)` runs a step on `n` workers and emits results in
  source order, so `MapCtx(f, WithParallel(8)).Collect()` returns what the sequential version
  would, only faster. One slow item holds back its successors; `WithUnordered()` gives that up for
  throughput when the consumer does not care about order.
- **Errors are data.** An error from a source or a callback flows down the pipeline as an element.
  Operators pass it along and keep working on values; terminals stop at the first one. An error is
  tolerated only when something says so, a policy such as `policy.CircuitBreaker` or a callback
  returning `Suppress(err)`, and it is marked rather than dropped, so nothing is lost.
- **Extensible where it matters.** `Transform` hands the raw `(value, error)` sequence to any
  function and `Suppress` marks an error as tolerated; that is the whole seam. The `policy`
  package is written on it and nothing else, so a policy in your module has the same tools.
- **Bugs are not data.** A panic in a callback, also on a worker goroutine, becomes an `ErrPanic`
  error that nothing may suppress. Whether a step runs on one worker or eight makes no difference
  to how a bug surfaces.
- **Native iterators in and out.** Sources are `iter.Seq`, `iter.Seq2[T, error]` or channels;
  `Seq()` gives the pipeline back as `iter.Seq2[T, error]`. Generators in `seq` are plain
  `iter.Seq`, usable with `slices.Collect` and `range` too.

Requires Go 1.27+ (generic methods, `iter`).

```bash
go get github.com/shults/chunkflow
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

More runnable examples, one per operation, are in `*_example_test.go` and on
[pkg.go.dev](https://pkg.go.dev/github.com/shults/chunkflow).

## API

### Building a stream

`New` binds the context; the source method picks the element type. Options come afterwards,
through `Opts` on the stream or on the individual `*Ctx` call.

| | |
| --- | --- |
| `New(ctx)` | Starts a `Stream` bound to `ctx`. |
| `.Seq(iter.Seq[T])` | Wraps a native iterator. The context is checked before every element. |
| `.Seq2(iter.Seq2[T, error])` | Wraps a `(value, error)` iterator, the inverse of `Stream.Seq()`. |
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

### Errors, in one place

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

## Performance

Measured, not claimed: [BENCHMARK.md](BENCHMARK.md). The short version: a sequential stage costs
about 10 ns per element and a constant handful of allocations per stream, a parallel stage about
half a microsecond per element in channel handoffs, so `WithParallel` pays once the callback
costs more than a few microseconds, which every I/O call does. Eight workers on 200 µs calls give
a 7.9× speed-up with source order kept, within 1% of `WithUnordered()`.

## Versioning

Pre-1.0. A patch release changes no exported API; a minor release adds API or breaks it, and every
break is listed in [CHANGELOG.md](CHANGELOG.md) with the migration. `v0.1.0` marks the core API
having seen real use outside this repository, not a date.

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
