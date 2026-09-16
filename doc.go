// Package chunkflow provides lazily evaluated, type-safe data pipelines on top of
// Go's native iterators (iter.Seq). It targets stream processing and chunked I/O
// work (ETL-style batching) and comes in two flavours that share one fluent API:
//
//   - Stream[T] is synchronous and infallible. Every intermediate operation is a
//     closure over the upstream iterator, nothing is buffered except by Chunk, and
//     nothing runs until a terminal operation pulls.
//   - IoStream[T] adds a context.Context, error propagation and optional worker
//     pools. Every Stream method exists on IoStream with the same name and shape;
//     terminal operations additionally return an error, and callbacks that need
//     the context or may fail have an *Async counterpart (MapAsync, FilterAsync,
//     ReduceAsync, AllAsync, AnyAsync, ForEachAsync).
//
// # Constructing streams
//
// Both types are built from native iterators only. Generators live in the
// sub-package seq (Items, Range, RangeInclusive, Numbers, Const, Repeat, Iterate) so that they are
// equally usable with slices.Collect, range loops and this package:
//
//	evens := chunkflow.NewStream(seq.Range(0, 100)).
//		Filter(func(i int) bool { return i%2 == 0 }).
//		Collect()
//
//	rows, err := chunkflow.NewIo(ctx, chunkflow.WithParallel(8)).Seq(seq.Items(ids...)).
//		MapAsync(fetchRow).
//		Chunk[[]Row](500).
//		ForEachAsync(insertBatch)
//
// IoStream is built through NewIo(ctx, opts...), which binds the context and default
// options first and lets the source pick the element type: Seq wraps an iter.Seq,
// Seq2 an iter.Seq2[T, error] (the inverse of IoStream.Seq), Chan a receive channel.
//
// # Type-changing operations
//
// Go methods cannot introduce type parameters that constrain the receiver, so
// operations such as Flatten (Stream[[]E] -> Stream[E]) are top-level functions.
// Through keeps the left-to-right reading order when using them:
//
//	stream.Chunk[[]int](3).Through(chunkflow.Flatten)
//	ioStream.Chunk[[]int](3).Through(chunkflow.IoFlatten)
//
// # Errors in IoStream
//
// An error, whether produced by the source or by a callback, travels down the
// pipeline as an element. Intermediate operations never interpret it: Map, Filter,
// Take, Skip, Chunk and IoFlatten forward it untouched and keep working on the
// values around it, so Take(n) and Skip(n) count values only. Terminal operations
// stop at the first error they see, which makes a plain pipeline fail fast and
// stop consuming the source right after the failing element.
//
// CircuitBreaker(n) is the one operator that tolerates errors. Below its threshold
// it re-emits each error wrapped so that errors.Is(err, ErrSuppressed) is true while
// the original error stays in the chain; terminal operations skip such elements,
// and consumers of Seq can still observe them. On the n-th consecutive error the
// breaker trips with a fatal error. Context errors are never suppressed.
//
// # Short-circuiting and cancellation
//
// Every operation honours the yield protocol: when a consumer stops (Take, First,
// Any, All, or a plain break in a range loop), the whole chain stops pulling from
// the source. In IoStream a consumer stopping, or the context being cancelled,
// also shuts down any worker pool spawned by MapAsync or FilterAsync; the
// cancellation itself is reported as an error by the terminal operation.
//
// # Options
//
// WithParallel(n) sets the worker count for *Async operations (default 1, ordered;
// with n > 1 the output order is not guaranteed). WithLogger and WithDiscardLogger
// control diagnostics. Options passed to Opts become defaults for every downstream
// operation; options passed to a single operation apply to that call only.
package chunkflow
