// Package chunkflow provides lazily evaluated, type-safe data pipelines on top of
// Go's native iterators (iter.Seq), aimed at stream processing and chunked I/O work
// (ETL-style batching). One type, Stream[T], carries a context.Context, propagates
// errors as elements and can run steps on worker pools. Every intermediate operation
// is a closure over the upstream iterator, nothing is buffered except by Chunk, and
// nothing runs until a terminal operation pulls. Callbacks that need the context or may
// fail have a *Ctx variant (MapCtx, FilterCtx, ReduceCtx, AllCtx, AnyCtx, ForEachCtx);
// the plain variant is that *Ctx variant pinned to one worker.
//
// # Constructing streams
//
// Streams are built from native iterators only. Generators live in the
// sub-package seq (Items, Range, RangeInclusive, Numbers, Const, Repeat, Iterate) so that they are
// equally usable with slices.Collect, range loops and this package:
//
//	evens, err := chunkflow.New(ctx).Seq(seq.Range(0, 100)).
//		Filter(func(i int) bool { return i%2 == 0 }).
//		Collect()
//
//	rows, err := chunkflow.New(ctx).Seq(seq.Items(ids...)).
//		MapCtx(fetchRow, chunkflow.WithParallel(8)).
//		Chunk[[]Row](500).
//		ForEachCtx(insertBatch)
//
// New(ctx) binds the context first and lets the source pick the element type: Seq wraps an iter.Seq, Seq2 an iter.Seq2[T, error] (the inverse
// of Stream.Seq), Chan a receive channel.
//
// # Type-changing operations
//
// Go methods cannot introduce type parameters that constrain the receiver, so
// operations such as Flatten (Stream[[]E] -> Stream[E]) are top-level functions.
// Through keeps the left-to-right reading order when using them:
//
//	stream.Chunk[[]int](3).Through(chunkflow.Flatten)
//
// # Errors
//
// An error, whether produced by the source or by a callback, travels down the
// pipeline as an element. Intermediate operations never interpret it: Map, Filter,
// Take, Skip, Chunk and Flatten forward it untouched and keep working on the
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
// A panic inside a callback is recovered where it happens, also on worker goroutines,
// and becomes an error matching ErrPanic that nothing may suppress. A bug therefore
// surfaces the same way whether the step runs sequentially or on WithParallel(n) workers.
//
// # Short-circuiting and cancellation
//
// Every operation honours the yield protocol: when a consumer stops (Take, First,
// Any, All, or a plain break in a range loop), the whole chain stops pulling from
// the source. In Stream a consumer stopping, or the context being cancelled,
// also shuts down any worker pool spawned by MapCtx or FilterCtx; the
// cancellation itself is reported as an error by the terminal operation.
//
// # Options
//
// Two kinds exist. A StepOption configures one *Ctx call: WithParallel(n) sets the
// worker count of MapCtx, FilterCtx or TapCtx (default 1; with n > 1 the output order of
// that step is not guaranteed). An Option configures the whole pipeline through Opts
// and is inherited downstream: every StepOption also works as an Option (a default
// worker count), and WithOnError(fn) registers the hook that terminal operations call for
// every error they handle, suppressed ones included, so logging and metrics need no
// manual loop over Seq.
package chunkflow
