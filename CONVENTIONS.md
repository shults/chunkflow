# Conventions

Decisions about naming and API shape, each with the reason it was made. Add an entry whenever a
new convention is introduced or an old one is changed; a convention without a reason is a habit.

## Naming

- **`*Ctx` suffix** marks the variant of an operation whose callback receives a `context.Context`
  and may return an error: `Map` / `MapCtx`, `ForEach` / `ForEachCtx`, `TakeWhile` / `TakeWhileCtx`.
  *Why:* the previous `*Async` suffix described behaviour these methods do not have (they block;
  concurrency is the separate `WithParallel` option). What differs is only the callback shape, and
  Go's own convention for "same operation, takes a context" is a context suffix (`ExecContext`,
  `DialContext`); `Ctx` is its short form. A suffix rather than a prefix keeps `Map` and `MapCtx`
  adjacent in godoc and in editor completion. The plain variant is a thin wrapper over the `*Ctx`
  one pinned to `WithParallel(1)`.
- **One stream type.** There is no separate in-memory, infallible stream. `Stream` carries a
  context and an error channel and is a strict superset; a second type meant every operator, test
  and doc twice and left semantic gaps that could not be closed (`Chunk(0)` panicking vs erroring,
  panics vs `ErrPanic`, no `Merge`). Pure in-memory pipelines pass `context.Background()` and ignore
  the error they know cannot happen. The removed type survives privately in `bench_test.go` as the
  reference for what the error/context plumbing costs (~2x on pure data, same allocations); if that
  ever matters to someone, that is the starting point for an opt-in type.
- **Names come from the standard library when std has the operation**: `Concat` (`slices.Concat`),
  `Compact` (`slices.Compact`), `Seq` / `Seq2` (`iter`). *Why:* a reader who knows std knows the
  semantics without opening godoc.
- **`Merge` vs `Concat`**: `Concat` is sequential and deterministic, `Merge` is a concurrent fan-in
  with interleaved order. Not `MergeOrdered` / `MergeUnordered`: "ordered merge" reads as the
  merge-sort step over sorted inputs.
- **Constructors**: the `New(ctx)` builder with `Seq`, `Seq2`, `Chan`. *Why:* the builder binds
  the context before the element type is known, and each source kind is one generic method
  instead of one more top-level `New*`. `New` takes no options: `Opts` on the stream is the only
  place for pipeline options, so there is one way to set them and the builder holds nothing but
  the context.
  There are no convenience constructors (`FromItems`, `Range`, ...) in the root package; generators
  live in `seq` and return plain `iter.Seq`, so they also work with `slices.Collect` and `range`.

## API shape

- **Operators work on values.** Intermediate operations (`Map`, `Filter`, `Take`, `Skip`, `Chunk`,
  `Compact`, `TakeWhile`, ...) never inspect or count errored elements; they forward them and keep
  processing. Invariant: `s.Op(...).Collect()` equals `Op` applied to `s.Collect()` as long as no
  fatal error occurs. *Why:* one rule for every operator instead of a per-operator debate; it is the
  only rule that closes over all operators (`Chunk` cannot put an error into a slice).
- **Errors flow, terminals decide.** An error is an element. Terminal operations stop at the first
  error that is not marked `ErrSuppressed`. Only `CircuitBreaker` marks errors as tolerated; it never
  drops or logs them, so `Seq()` shows everything. *Why:* silent data loss is the worst failure mode
  of a pipeline; observability stays with the consumer.
- **Panics in callbacks become `ErrPanic` errors, never re-panics.** Recovered once per stage
  (also inside workers) with the callback boundary marked by a bool, pushed downstream in position,
  never suppressed by `CircuitBreaker`, returned by every terminal. *Why:* `recover` only works on
  the panicking goroutine, so a worker panic can only be reported as data; doing the same on the
  sequential path keeps `WithParallel(1)` and `WithParallel(8)` — and `Seq()` consumers — behaving
  identically. One guard per stage instead of per element: the per-element `defer recover()` cost
  half of the pipeline's CPU time in benchmarks.
- **Type-changing operations are top-level functions used via `Through`.** A method cannot add a
  constraint on the receiver's type parameter (`Flatten` needs `Stream[[]E]`, `Compact` needs
  `comparable`), and Go has no overloading. `Through` keeps the left-to-right reading order.
- **Generic methods introduce new type parameters** (`Map[R]`, `Chunk[R]`, `Seq[T]` on the builder)
  wherever the receiver's own parameters suffice as constraints. This is the language feature the
  library is built on; use it before reaching for a top-level function.
- **Minimal exported surface.** No struct is exported to carry internal state (the breaker's
  suppressed-error struct is private; only the `ErrSuppressed` sentinel is public). An exported type
  is a one-way door: adding one later is free, removing one is a breaking change.
- **Rule of three.** Do not generalise from one case. `CircuitBreaker` stays a concrete operator until
  a second error policy shows up; `Zip3` is not added until someone needs it; there is no `Distinct`
  because a global "seen" set belongs to the user's store, not inside the pipeline
  (see `ExampleStream_Chunk_deduplication`).
- **Two option kinds, checked by the compiler.** `Option` configures a whole pipeline (`Opts`)
  and is inherited downstream; `StepOption` configures one `*Ctx` call and is also an
  `Option`. `WithParallel` is a `StepOption`, `WithOnError` a pipeline-only `Option`, so
  `MapCtx(fn, WithOnError(...))` and `ReduceCtx(init, fn, WithParallel(4))` do not compile.
  *Why:* an accepted-but-ignored option is a silent bug; the earlier single `Option` type needed a
  logged warning for exactly that case, and the logger existed for nothing else.
- **Invalid option values are not panics and not nil checks.** `WithParallel(0)` or
  `WithOnError(nil)` record an error in the options; the first operation that consumes them
  (`Opts` or a `*Ctx` step) emits that error and ends. Defaults are
  real values (`onError` is a no-op func), so hot paths never test for nil.
- **No logger in the API.** Observability of tolerated errors goes through `WithOnError` (or a
  manual loop over `Seq()`), never through a library-owned `slog.Logger`. *Why:* the library has
  nothing to say on its own; the user decides whether an error becomes a log line or a metric.
- **Options are never typed by the element.** Anything that depends on `T` (`Reduce`'s init value)
  is a positional parameter; `options` stays a plain struct shared by every pipeline.
- **Positional parameters before closures.** `Reduce(init, fn)`, not `Reduce(fn, init)`: a value
  trailing a multi-line func literal reads badly.
- **Fold callbacks take `(acc, item)`**, as in `slices.Reduce` (x/exp), `lo.Reduce` and most
  languages' `fold`; the accumulator type is a separate type parameter `R`. *Why:* the accumulator
  is the thing being built, so it comes first, and forcing `R == T` made common folds (sum of
  lengths, building a map) impossible without a preceding `Map`.

## Semantics worth writing down

- `All` on an empty stream is `true`, `Any` is `false` (vacuous truth); `All(p) == !Any(!p)` always.
- `Take(n)` / `Skip(n)` count values, not errors. `TakeWhile` stops pulling at the first `false`
  and therefore never sees errors that come after it.
- `Compact` removes only *adjacent* duplicates (input must be sorted or grouped), in O(1) memory.
- `Chan` streams are single-use and stopping early does not close or drain the channel.
- Combinators over several `Stream`s (`Concat`, `Merge`) take values and deadline from the
  first stream's context and are cancelled by any stream's context, with the original cause kept.
- Cancellation always surfaces as an error element, also when a worker pool exits without emitting.

## Code layout

- `stream.go`: exported types and constructors, then exported methods (intermediate,
  then terminal), then exported functions, then everything unexported (types, functions, methods).
- Every derived `Stream` is built through `derive` / `errStream`; every terminal goes through
  `each`. Errored elements are forwarded with `if !yield(...) { return }; continue`.

## Testing

- `go test -race`, `goleak.VerifyTestMain` for the package plus `goleak.VerifyNone` in every test
  that starts goroutines, 100% statement coverage as the working target, runnable `Example*` tests
  with `// Output:` for every public operation. A design claim is demonstrated with a failing test
  or a mutation check before it is argued.
