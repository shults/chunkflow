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
)

func main() {

    res := chunkflow.
        Numbers(1). // 1. Generate an infinite stream, but Take(20) and Skip(5) limit consumption to exactly 20 elements.
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

### Generators (Initialization)

Functions that instantiate a new `Stream[T]`.

| Function | Description | Stream Type |
| --- | --- | --- |
| `NewStream(iter.Seq[T])` | Wraps a native Go iterator. | Any |
| `FromItems(...T)` | Converts variadic arguments into a finite stream. | Finite |
| `FromRange(start, end)` | Generates integers in the `[start, end)` range. | Finite |
| `Numbers(start)` | Generates a monotonically increasing sequence. | **Infinite** |
| `Const(val)` | Repeatedly emits the exact same value. | **Infinite** |

### Intermediate Operations (Lazy Transformations)

These modify the pipeline logic but execute absolutely no work until a terminal operation is invoked.

| Method | Signature | Description |
| --- | --- | --- |
| `Map` | `Map[R](func(T) R)` | Transforms each element from type T to R. |
| `Filter` | `Filter(func(T) bool)` | Emits only elements that satisfy the predicate. |
| `Take` | `Take(int)` | Limits the stream to the first N elements. |
| `Skip` | `Skip(int)` | Bypasses the first N elements. |
| `Chunk` | `Chunk[R ~[]T](int)` | Groups elements into physical slices of the given size. |
| `Through` | `Through[R](func(Stream) Stream)` | Pipes the stream through an external top-level function. |

### Top-Level Functions

| Function | Signature | Description |
| --- | --- | --- |
| `Flatten` | `Flatten[E](Stream[[]E])` | Unwraps a stream of slices into a flat stream of elements. Use with `.Through()`. |

### Terminal Operations (Execution Triggers)

These pull the trigger, forcing the intermediate pipeline to evaluate.

| Method | Return Type | Description | Warning |
| --- | --- | --- | --- |
| `Collect` | `[]T` | Allocates and returns all processed elements as a slice. | OOM risk for infinite streams |
| `Reduce` | `T` | Aggregates elements into a single accumulated value. | Requires a finite stream |
| `ForEach` | `void` | Executes a side effect for every element. | Blocks the thread until completion |
| `Exec` | `void` | Exhausts the stream entirely, discarding values. | - |
| `Any` | `bool` | Short-circuits and returns true on the first match. | - |
| `All` | `bool` | Short-circuits and returns false on the first mismatch. | - |
| `First` | `(T, bool)` | Retrieves the first element and short-circuits. | - |
| `Last` | `(T, bool)` | Consumes the entire stream to return the final element. | Requires a finite stream |

## Design Notes: Why `Flatten` requires `Through`

Due to strict constraints in the Go compiler, it is impossible to apply narrower type constraints to method receivers (e.g., enforcing that `T` must be a slice `[]E` for a specific method).

To prevent runtime panics caused by empty interface assertions, `Flatten` is implemented strictly as a top-level function. The `Through` method acts as a structural bridge to inject this top-level function directly into the pipeline without breaking the Fluent API chain.

```go
// Instead of breaking the chain:
stream2 := chunkflow.Flatten(stream1.Chunk(100))

// Maintain strict left-to-right flow readability:
stream1.Chunk[[]int](100).Through(chunkflow.Flatten[int])

```