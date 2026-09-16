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
- **`Io` prefix** marks the `IoStream` counterpart of a top-level `Stream` function: `Flatten` /
  `IoFlatten`, `Compact` / `IoCompact`, `Concat` / `IoConcat`. *Why:* Go has no overloading, and
  the prefix mirrors the type name. Functions that exist only for `IoStream` (`IoMerge`) carry it too.
- **Names come from the standard library when std has the operation**: `Concat` (`slices.Concat`),
  `Compact` (`slices.Compact`), `Seq` / `Seq2` (`iter`). *Why:* a reader who knows std knows the
  semantics without opening godoc.
- **`Merge` vs `Concat`**: `Concat` is sequential and deterministic, `Merge` is a concurrent fan-in
  with interleaved order. Not `MergeOrdered` / `MergeUnordered`: "ordered merge" reads as the
  merge-sort step over sorted inputs, and the two are not symmetric (`Merge` needs goroutines, so
  there is no `Stream` variant).
- **Constructors**: `NewStream(seq)` for `Stream`; `NewIo(ctx, opts...)` builder with `Seq`, `Seq2`,
  `Chan` for `IoStream`. *Why:* the builder binds context and default options before the element
  type is known, and each source kind is one generic method instead of one more top-level `New*`.
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
- **Type-changing operations are top-level functions used via `Through`.** A method cannot add a
  constraint on the receiver's type parameter (`Flatten` needs `Stream[[]E]`, `Compact` needs
  `comparable`). `Through` keeps the left-to-right reading order.
- **Generic methods introduce new type parameters** (`Map[R]`, `Chunk[R]`, `Seq[T]` on the builder)
  wherever the receiver's own parameters suffice as constraints. This is the language feature the
  library is built on; use it before reaching for a top-level function.
- **Minimal exported surface.** No struct is exported to carry internal state (the breaker's
  suppressed-error struct is private; only the `ErrSuppressed` sentinel is public). An exported type
  is a one-way door: adding one later is free, removing one is a breaking change.
- **Rule of three.** Do not generalise from one case. `CircuitBreaker` stays a concrete operator until
  a second error policy shows up; `Zip3` is not added until someone needs it; there is no `Distinct`
  because a global "seen" set belongs to the user's store, not inside the pipeline
  (see `ExampleIoStream_Chunk_deduplication`).
- **Options are shared, not generic.** `Option` configures concurrency and logging for a whole
  pipeline (`NewIo(ctx, opts...)`, `Opts`) or one call (`MapCtx(fn, opts...)`). Anything typed by the
  element (`Reduce`'s init value) is a positional parameter, never an option.
- **Positional parameters before closures.** `Reduce(init, fn)`, not `Reduce(fn, init)`: a value
  trailing a multi-line func literal reads badly.

## Semantics worth writing down

- `All` on an empty stream is `true`, `Any` is `false` (vacuous truth); `All(p) == !Any(!p)` always.
- `Take(n)` / `Skip(n)` count values, not errors. `TakeWhile` stops pulling at the first `false`
  and therefore never sees errors that come after it.
- `Compact` removes only *adjacent* duplicates (input must be sorted or grouped), in O(1) memory.
- `Chan` streams are single-use and stopping early does not close or drain the channel.
- Combinators over several `IoStream`s (`IoConcat`, `IoMerge`) take values and deadline from the
  first stream's context and are cancelled by any stream's context, with the original cause kept.
- Cancellation always surfaces as an error element, also when a worker pool exits without emitting.

## Code layout

- `iostream.go` / `stream.go`: exported types and constructors, then exported methods (intermediate,
  then terminal), then exported functions, then everything unexported (types, functions, methods).
- Every derived `IoStream` is built through `derive` / `errStream`; every terminal goes through
  `each`. Errored elements are forwarded with `if !yield(...) { return }; continue`.

## Testing

- `go test -race`, `goleak.VerifyTestMain` for the package plus `goleak.VerifyNone` in every test
  that starts goroutines, 100% statement coverage as the working target, runnable `Example*` tests
  with `// Output:` for every public operation. A design claim is demonstrated with a failing test
  or a mutation check before it is argued.
