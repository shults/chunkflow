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
- **`Zip` takes a callback and there is no tuple type.** `Zip(other, fn)` hands both values to fn
  and emits what fn returns; a third source is another `Zip` whose fn extends the struct the first
  one built. *Why:* Go cannot grow a struct per zip level, so a generic `Pair` ends in
  `p.First.First.Second` after three sources, while a callback names the fields on the first
  step and keeps naming them. `Zip` is a method, not a function, because a generic method can
  introduce `O` and `R` and infer them from the arguments, so unlike `CircuitBreaker` no type
  argument is spelled out.
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
- **Parallel steps keep source order by default.** `WithParallel(n)` reorders results behind a
  bounded window (`2n` items pulled and not yet emitted, the multiplier lives in `options` and has
  no public setter until someone needs one); `WithUnordered()` opts out. *Why:* it keeps the
  invariant above without an exception for parallel stages, and it matches what every parallel-map
  API defaults to (`Pool.map` vs `imap_unordered`, rayon's `collect`, Java's ordered
  `parallelStream().toList()`): correctness by default, throughput by choice. The price,
  head-of-line blocking, is documented on `WithParallel` and is paid only by pipelines that would
  otherwise have had to sort.
- **Errors flow, terminals decide.** An error is an element. Terminal operations stop at the first
  error that is not marked `ErrSuppressed`. Only `Suppress` marks an error, whether called by a
  callback or by a policy such as `policy.CircuitBreaker`; nobody drops or logs it, so `Seq()`
  shows everything. *Why:* silent data loss is the worst failure mode of a pipeline; observability stays
  with the consumer. Context errors and `ErrPanic` are refused by both, at construction, so an error
  that matches `ErrSuppressed` is always one the terminals will skip.
- **The error model lives in the root, policies do not.** The root owns the sentinels
  (`ErrSuppressed`, `ErrPanic`, `ErrEmpty`), `Suppress` and the seam `Transform`; that is the whole
  vocabulary for tolerating errors. Everything that *decides* which errors to tolerate, starting
  with `CircuitBreaker`, lives in `policy` and is written on that public seam and nothing else.
  *Why:* the first policy walks the same path a third-party extension would, so the seam is proven
  sufficient by construction, and new policies are a minor version, never a breaking change.
- **Panics in callbacks become `ErrPanic` errors, never re-panics.** Recovered once per stage
  (also inside workers) with the callback boundary marked by a bool, pushed downstream in position,
  refused by `Suppress`, returned by every terminal. *Why:* `recover` only works on
  the panicking goroutine, so a worker panic can only be reported as data; doing the same on the
  sequential path keeps `WithParallel(1)` and `WithParallel(8)` — and `Seq()` consumers — behaving
  identically. One guard per stage instead of per element: the per-element `defer recover()` cost
  half of the pipeline's CPU time in benchmarks.
- **Type-changing operations are top-level functions used via `Through`.** A method cannot add a
  constraint on the receiver's type parameter (`Flatten` needs `Stream[[]E]`, `Compact` needs
  `comparable`), and Go has no overloading. `Through` keeps the left-to-right reading order.
- **Error policies are functions used via `Through`, too.** `policy.CircuitBreaker[T](n)` is not
  a method although a method would compile: it does not operate on values, it decides what an error
  means, and every policy, in this module or outside it, should enter the chain the same way. The
  price is an explicit type argument, `Through(policy.CircuitBreaker[User](5))`, because Go does
  not infer a call's type parameters from where its result is used (checked; a generic method value
  on an exported config type would infer, but exports a type for nothing).
- **The extension seam is `iter.Seq2[T, error]`, not an exported element type.** `Transform` speaks
  the same `(value, error)` pairs as `Seq()` and `Seq2`, so an extension author writes an ordinary
  `for v, err := range in` loop and no chunkflow type appears in the signature. *Why:* an exported
  `Result[T]` would be a one-way door for a struct that carries no behaviour; the native pair is
  already the representation at every other boundary.
- **Two extension families, two seams.** A *policy* is `func(Stream[T]) Stream[T]`, enters through
  `Through`, sees results and their position in the chain and can tolerate, replace or end, but
  cannot re-run anything (`policy.CircuitBreaker`). A *step decorator* wraps the callback itself,
  `func(ctx, T) (R, error)` in and out, enters as the argument of `MapCtx`, sees the call and can
  repeat it, bound it in time or measure it (`Retry`, `Timeout`). A decorator needs nothing from
  this module, the callback signature is plain Go, so it can live anywhere; it must not retry
  context errors or errors already marked by `Suppress`, since the caller has decided. The two
  compose: `MapCtx(retry(3, fetch), WithParallel(8)).Through(policy.CircuitBreaker[User](5))` is
  three attempts per element, then a breaker over five elements in a row. *Why:* one mechanism
  cannot express both, a stream transform sees only outcomes, a wrapper sees only one call.
- **Generic methods introduce new type parameters** (`Map[R]`, `Chunk[R]`, `Seq[T]` on the builder)
  wherever the receiver's own parameters suffice as constraints. This is the language feature the
  library is built on; use it before reaching for a top-level function.
- **Minimal exported surface.** No struct is exported to carry internal state (the breaker's
  suppressed-error struct is private; only the `ErrSuppressed` sentinel is public). An exported type
  is a one-way door: adding one later is free, removing one is a breaking change.
- **Rule of three.** Do not generalise from one case; it applies to exported names and
  abstractions, where a wrong guess is a breaking change to undo. It does not apply to private
  helpers: a long function is split when it stops fitting in one head, and a helper used once is
  fine when it names a step (`mergeCtx`, `chain`, `neverSuppressed` were all extracted for size,
  not for reuse). `policy.CircuitBreaker` stays the only policy
  until a second one has a use case; `Zip3` is not added until someone needs it; there is no `Distinct`
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
- `ReduceBy` is the only keyed operation and it is a terminal, like `Collect`: terminals may
  materialise, intermediate operators may not (that is why there is no `Distinct` or streaming
  `GroupBy`). `init` is copied into every group by value; a reference type as `init` is shared
  between groups, which is the caller's bug, not a case the library papers over.
- `First` / `Last` return `(T, error)` and signal a stream without values with the `ErrEmpty`
  sentinel, not with a `bool`. *Why:* the function returns an error anyway, so a third return
  value forces two checks where one suffices, and std signals expected absence from an
  error-returning function with a sentinel (`sql.ErrNoRows`, `io.EOF`, `fs.ErrNotExist`); the
  `(v, ok)` form is for functions that cannot fail (maps, `iter.Pull`). `ErrEmpty` is not an element
  error and never reaches `WithOnError`.
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
