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

## API Reference

### Constructors

`Stream[T]` and `IoStream[T]` have exactly one way of being created: wrapping a native Go iterator.

| Function | Description |
| --- | --- |
| `NewStream(iter.Seq[T])` | Wraps a native iterator into a synchronous `Stream[T]`. |
| `NewIoStream(ctx, iter.Seq[T])` | Wraps a native iterator into a context-aware, error-propagating `IoStream[T]`. |
| `NewIoStream2(ctx, iter.Seq2[T, error])` | Wraps a `(value, error)` iterator into an `IoStream[T]`. Inverse of `IoStream.Seq()`. |

### Generators (`seq` sub-package)

Iterator generators live in `github.com/shults/chunkflow/seq`. They return plain `iter.Seq[T]`, so they work with `NewStream`, `NewIoStream`, `slices.Collect` and `range` loops alike.

| Function | Description | Length |
| --- | --- | --- |
| `seq.Items(...T)` | Yields the variadic arguments. | Finite |
| `seq.Range(from, to)` | Yields integers in the half-open interval `[from, to)`. Works for any integer type. | Finite |
| `seq.RangeInclusive(from, to)` | Yields integers in the closed interval `[from, to]`. Safe for `to == MaxInt`. | Finite |
| `seq.Numbers(start)` | Monotonically increasing integers starting at `start`. | **Infinite** |
| `seq.Const(val)` | Repeatedly emits the same value. | **Infinite** |

### Intermediate Operations (Lazy Transformations)

These modify the pipeline logic but execute absolutely no work until a terminal operation is invoked.
Every method below exists with the same name and shape on both `Stream[T]` and `IoStream[T]`.

| Method | Signature | Description |
| --- | --- | --- |
| `Map` | `Map[R](func(T) R)` | Transforms each element from type T to R. |
| `Filter` | `Filter(func(T) bool)` | Emits only elements that satisfy the predicate. |
| `Take` | `Take(int)` | Limits the stream to the first N elements. |
| `Skip` | `Skip(int)` | Bypasses the first N elements. |
| `Chunk` | `Chunk[R ~[]T](int)` | Groups elements into physical slices of the given size. |
| `Through` | `Through[R](func(Stream) Stream)` | Pipes the stream through an external top-level function. |

`IoStream[T]` additionally offers context-aware, error-returning variants and concurrency:

| Method | Signature | Description |
| --- | --- | --- |
| `MapAsync` | `MapAsync[R](func(ctx, T) (R, error), ...Option)` | Like `Map`, but may fail and run on `WithParallel(n)` workers (order not preserved for n > 1). |
| `FilterAsync` | `FilterAsync(func(ctx, T) (bool, error), ...Option)` | Like `Filter`, with the same error and concurrency semantics as `MapAsync`. |
| `CircuitBreaker` | `CircuitBreaker(maxConsecutiveFailures int)` | Suppresses errors until `n` occur in a row, then aborts the pipeline. |
| `Opts` | `Opts(...Option)` | Sets default options (`WithParallel`, `WithLogger`) inherited by all downstream operations. |

### Top-Level Functions

| Function | Signature | Description |
| --- | --- | --- |
| `Flatten` | `Flatten[E](Stream[[]E])` | Unwraps a stream of slices into a flat stream of elements. Use with `.Through()`. |
| `IoFlatten` | `IoFlatten[E](IoStream[[]E])` | `IoStream` counterpart of `Flatten`. Use with `.Through()`. |

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
| `Any` | `bool` | `(bool, error)` | Short-circuits and returns true on the first match. | - |
| `All` | `bool` | `(bool, error)` | Short-circuits and returns false on the first mismatch. | - |
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