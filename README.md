# ChunkFlow

**ChunkFlow** is a rigorous, lazily evaluated data pipeline for Go 1.27+. It wraps the native `iter.Seq` interface, providing a type-safe Fluent API tailored for stream processing, transformations, and data chunking for I/O operations (ETL).

## Architecture & Constraints

* **O(1) Memory Complexity:** All intermediate transformations (except `Chunk`) are pure closures. They do not allocate heap memory and operate on the stream on the fly.
* **Short-circuiting:** Terminal operations like `Take`, `Any`, `All`, or `First` instantly halt the consumption of the source stream, preventing wasted CPU cycles.
* **Zero Reflection:** The package relies entirely on Go 1.27+ generics and generic methods (with necessary compiler workarounds detailed below).
* **Requirements:** Go 1.27+ or higher (due to the strict dependency on the native `iter` package).

## Installation

```bash
go get github.com/shults/chunkflow

```

## Quick Start

A practical example demonstrating lazy evaluation, chunking (e.g., to prevent N+1 database query problems), and flattening the stream back to its original shape:

```go
package main

import (
    "fmt"
    "github.com/shults/chunkflow"
    "github.com/shults/chunkflow/seq"
)

func main() {

    res := chunkflow.
        NewStream(seq.Numbers(1)). // 1. Wrap an infinite iterator; Skip(5) and Take(20) limit consumption to exactly 25 elements.
        Skip(5).
        Take(20).
        Filter(func(i int) bool { // 2. Lazy filtering on the fly.
            return i%2 == 0
        }).
        Chunk[[]int](3). // 3. Buffer elements into chunks of 3.
        Through(chunkflow.Flatten[int]). // 4. Unwrap the chunks back into a flat stream using the architectural bridge.
        Collect() // 5. Materialize the final result into a physical slice.

    fmt.Println(res)
}

```

## Development

```bash
make setup   # installs golangci-lint and govulncheck into ./bin, enables the git pre-commit hook
make check   # gofmt, go vet, golangci-lint, go mod tidy — what the hook runs (~0.5s warm)
make test    # go test -race -shuffle=on
make ci      # check + test + govulncheck, mirrors the GitHub Actions pipeline
```

The pre-commit hook lives in `.githooks/` and is enabled via `core.hooksPath`. Skip it once with `git commit --no-verify`.

## API Reference

### Constructors

`Stream[T]` is created from a native iterator. `IoStream[T]` is created through a small builder:
`NewIo` binds the context and default options first, and the source method picks the element type.

| Function | Description |
| --- | --- |
| `NewStream(iter.Seq[T])` | Wraps a native iterator into a synchronous `Stream[T]`. |
| `NewIo(ctx, ...Option)` | Starts an `IoStream` bound to `ctx`; options become the pipeline defaults. |
| `NewIo(ctx).Seq(iter.Seq[T])` | Wraps a native iterator. The context is checked before every element. |
| `NewIo(ctx).Seq2(iter.Seq2[T, error])` | Wraps a `(value, error)` iterator. Inverse of `IoStream.Seq()`. |
| `NewIo(ctx).Chan(<-chan T)` | Reads a channel until it is closed or the context is cancelled. Single-use; stopping early does not close the channel. |

### Generators (`seq` sub-package)

Iterator generators live in `github.com/shults/chunkflow/seq`. They return plain `iter.Seq[T]`, so they work with `NewStream`, `NewIo(ctx).Seq`, `slices.Collect` and `range` loops alike.

| Function | Description | Length |
| --- | --- | --- |
| `seq.Items(...T)` | Yields the variadic arguments. | Finite |
| `seq.Range(from, to)` | Yields integers in the half-open interval `[from, to)`. Works for any integer type. | Finite |
| `seq.RangeInclusive(from, to)` | Yields integers in the closed interval `[from, to]`. Safe for `to == MaxInt`. | Finite |
| `seq.Numbers(start)` | Monotonically increasing integers starting at `start`. | **Infinite** |
| `seq.Const(val)` | Repeatedly emits the same value. | **Infinite** |
| `seq.Repeat(val, n)` | Emits the same value `n` times; the finite form of `Const`. | Finite |
| `seq.Iterate(seed, fn)` | Emits `seed`, `fn(seed)`, `fn(fn(seed))`, ... Restarts from `seed` on every pass. | **Infinite** |

### Intermediate Operations (Lazy Transformations)

These modify the pipeline logic but execute absolutely no work until a terminal operation is invoked.
Every method below exists with the same name and shape on both `Stream[T]` and `IoStream[T]`.

| Method | Signature | Description |
| --- | --- | --- |
| `Map` | `Map[R](func(T) R)` | Transforms each element from type T to R. |
| `Filter` | `Filter(func(T) bool)` | Emits only elements that satisfy the predicate. |
| `Tap` | `Tap(func(T))` | Runs a side effect for each element and passes it through unchanged. |
| `Take` | `Take(int)` | Limits the stream to the first N elements. |
| `Skip` | `Skip(int)` | Bypasses the first N elements. |
| `TakeWhile` | `TakeWhile(func(T) bool)` | Emits elements while the predicate holds, then stops pulling from the source. |
| `SkipWhile` | `SkipWhile(func(T) bool)` | Drops elements while the predicate holds, then emits the rest without testing. |
| `CompactFunc` | `CompactFunc(func(a, b T) bool)` | Drops **consecutive** duplicates in O(1) memory; input must be sorted or grouped for a global dedup. |
| `Chunk` | `Chunk[R ~[]T](int)` | Groups elements into physical slices of the given size. |
| `Through` | `Through[R](func(Stream) Stream)` | Pipes the stream through an external top-level function. |

`IoStream[T]` additionally offers context-aware, error-returning variants and concurrency:

| Method | Signature | Description |
| --- | --- | --- |
| `MapAsync` | `MapAsync[R](func(ctx, T) (R, error), ...Option)` | Like `Map`, but may fail and run on `WithParallel(n)` workers (order not preserved for n > 1). |
| `FilterAsync` | `FilterAsync(func(ctx, T) (bool, error), ...Option)` | Like `Filter`, with the same error and concurrency semantics as `MapAsync`. |
| `TapAsync` | `TapAsync(func(ctx, T) error, ...Option)` | Like `Tap`; a returned error replaces the element. Same concurrency semantics as `MapAsync`. |
| `TakeWhileAsync` / `SkipWhileAsync` | `(func(ctx, T) (bool, error))` | Context-aware predicates; always sequential. Errors in the stream pass through unevaluated. |
| `CircuitBreaker` | `CircuitBreaker(maxConsecutiveFailures int)` | Tolerates up to `n-1` errors in a row by re-emitting them marked as `ErrSuppressed`; trips on the `n`-th. Context errors are never suppressed. |
| `Opts` | `Opts(...Option)` | Sets default options (`WithParallel`, `WithLogger`, `WithDiscardLogger`) inherited by all downstream operations. |

#### Error semantics in `IoStream`

An error from the source or from a callback travels down the pipeline **as an element**. Intermediate
operations pass it through and keep processing the remaining input (`Take`/`Skip` do not count errors,
`Chunk` keeps its partial buffer). Terminal operations stop at the first error they see, so a pipeline
without a `CircuitBreaker` fails fast, and the source is not consumed past the failing element.

`CircuitBreaker` does not drop errors. Below its threshold it re-emits each error wrapped so that it
matches both `ErrSuppressed` and the original error. Terminal operations
skip such elements instead of stopping, so nothing is logged and nothing is lost: iterate `Seq()`
and check `errors.Is(err, chunkflow.ErrSuppressed)` to observe what was tolerated.

```go
for v, err := range stream.CircuitBreaker(5).Seq() {
    switch {
    case err == nil:
        use(v)
    case errors.Is(err, chunkflow.ErrSuppressed):
        metrics.Inc("tolerated") // original error is still in the chain: errors.Is(err, io.EOF) works
    default:
        return err // fatal: breaker tripped, context cancelled, ...
    }
}
```

### Top-Level Functions

| Function | Signature | Description |
| --- | --- | --- |
| `Flatten` | `Flatten[E](Stream[[]E])` | Unwraps a stream of slices into a flat stream of elements. Use with `.Through()`. |
| `IoFlatten` | `IoFlatten[E](IoStream[[]E])` | `IoStream` counterpart of `Flatten`. Use with `.Through()`. |
| `Compact` | `Compact[T comparable](Stream[T])` | `CompactFunc` with `==`. Needs `comparable`, hence top-level. Use with `.Through()`. |
| `IoCompact` | `IoCompact[T comparable](IoStream[T])` | `IoStream` counterpart of `Compact`. Use with `.Through()`. |
| `Concat` | `Concat(...Stream[T])` | Emits the streams one after another, deterministic order. Name follows `slices.Concat`. |
| `IoConcat` | `IoConcat(...IoStream[T])` | `IoStream` counterpart of `Concat`; errors keep their position. |
| `IoMerge` | `IoMerge(...IoStream[T])` | Consumes all streams concurrently and interleaves elements as they arrive. No `Stream` variant: needs goroutines and a context. |

### Terminal Operations (Execution Triggers)

These pull the trigger, forcing the intermediate pipeline to evaluate. On `IoStream[T]` every terminal
operation additionally returns an `error`: the first error produced upstream (or by the callback) stops
consumption and is returned.

| Method | `Stream` returns | `IoStream` returns | Description | Warning |
| --- | --- | --- | --- | --- |
| `Collect` | `[]T` | `([]T, error)` | Materializes all processed elements into a slice. | OOM risk for infinite streams |
| `Reduce` | `T` | `(T, error)` | Aggregates elements into a single accumulated value. | Requires a finite stream |
| `ForEach` | - | `error` | Executes a side effect for every element. | Blocks until completion |
| `Exec` | - | `error` | Exhausts the stream, discarding values. | - |
| `Count` | `int` | `(int, error)` | Consumes the stream and returns the number of elements. | Requires a finite stream |
| `Any` | `bool` | `(bool, error)` | Short-circuits and returns true on the first match. **Empty stream: `false`.** | - |
| `All` | `bool` | `(bool, error)` | Short-circuits and returns false on the first mismatch. **Empty stream: `true`** (vacuous truth). | - |
| `First` | `(T, bool)` | `(T, bool, error)` | Retrieves the first element and short-circuits. | - |
| `Last` | `(T, bool)` | `(T, bool, error)` | Consumes the entire stream to return the final element. | Requires a finite stream |
| `Seq` | `iter.Seq[T]` | `iter.Seq2[T, error]` | Exposes the pipeline as a native iterator. | - |

`IoStream[T]` also provides `ReduceAsync`, `AllAsync`, `AnyAsync` and `ForEachAsync`, whose callbacks
receive the `context.Context` and may return an error.

## Design Notes: Why `Flatten` requires `Through`

Due to strict constraints in the Go compiler, it is impossible to apply narrower type constraints to method receivers (e.g., enforcing that `T` must be a slice `[]E` for a specific method).

To prevent runtime panics caused by empty interface assertions, `Flatten` is implemented strictly as a top-level function. The `Through` method acts as a structural bridge to inject this top-level function directly into the pipeline without breaking the Fluent API chain.

```go
// Instead of breaking the chain:
stream2 := chunkflow.Flatten(stream1.Chunk(100))

// Maintain strict left-to-right flow readability:
stream1.Chunk[[]int](100).Through(chunkflow.Flatten[int])

```