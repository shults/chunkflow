# Roadmap

Working TODO list for ChunkFlow. Phases are finished (or consciously deferred) in order.
Design decisions and their reasons live in [CONVENTIONS.md](CONVENTIONS.md); this file only
tracks what is left to do. Items that were removed rather than done are kept struck through with
the reason, so the question is not reopened by accident.

## Done (v0.0.1 → v0.0.2)

- Safety net: CI (gofmt, vet, golangci-lint, race tests on 1.27.x and stable, govulncheck),
  `Makefile` + versioned pre-commit hook, `goleak` on every concurrent test, 100% statement
  coverage, runnable `Example*` tests, `doc.go`.
- One stream type. The in-memory `Stream` was removed and `IoStream` took its name; the old type
  survives privately in `bench_test.go` as the baseline for what error/context plumbing costs.
- Error model: errors travel as elements, operators work on values only, terminals stop at the
  first error, `CircuitBreaker` marks tolerated errors with `ErrSuppressed`, panics in callbacks
  become `ErrPanic` (never suppressed), `WithOnError` hook on terminals.
- `New(ctx, opts...)` builder with `Seq`, `Seq2`, `Chan`; `Option` / `StepOption` split;
  `WithParallel`, `WithOnError`; the logger is gone.
- Operators: `Tap`, `TakeWhile`, `SkipWhile`, `Compact`/`CompactFunc`, `Concat`, `Merge` (with
  merged contexts), `Count`; `*Ctx` naming; `Reduce[R]` with `(acc, item)`.
- `seq`: `Items`, `Range`, `RangeInclusive`, `Numbers`, `Const`, `Repeat`, `Iterate`.
- ~~`Distinct`~~ — a global "seen" set belongs to the user's store; the batched pattern is
  `ExampleStream_Chunk_deduplication`.
- ~~`WithLogger` / `WithDiscardLogger`~~ — existed for one warning; replaced by `WithOnError`.

## Phase 3 — API polish (breaking, before v0.1.0)

Goal: fix the shapes that are awkward now, while nobody depends on them, then freeze.

- [x] `New(ctx)` without options. `New(ctx, opts...)` and `Stream.Opts(opts...)` were two ways to do
      one thing; the source methods (`Seq`, `Seq2`, `Chan`) use no options, so nothing is lost by
      `New(ctx).Chan(ch).Opts(WithParallel(8))`. The builder now holds only the context
- [x] ordered parallel steps. Planned as an opt-in `WithOrdered()`; shipped the other way round:
      `WithParallel(n > 1)` keeps source order by default and `WithUnordered()` opts out (reasons
      in CONVENTIONS.md). Implementation: one-shot result channel per element pushed in order
      into a queue, a window semaphore of `k = 2n` permits bounds items pulled and not yet
      emitted, upstream errors get a pre-filled ticket and keep their position. The multiplier
      is a field in `options` with no public setter; ~~`WithWindow(k)`~~ waits in the parking lot
      until someone with a lot of RAM and high I/O variance asks for it. Tests: order under random
      delays, window bound, errors in source order through `CircuitBreaker`, panic at its position,
      `Take(1)`, cancellation, goleak everywhere; the unordered pool keeps its own tests
- [x] API trim review before the freeze — every exported name must defend its place. Outcome:
  - `Count()` stays: a one-line `Reduce`, but the most common terminal after `Collect` and present
    in every stream library
  - ~~`Exec()`~~ renamed `Drain()`: the role (terminal for side-effect pipelines) is real, the name
    was not; `Drain` is the established term, `Eval` would suggest a computed result
  - `Compact` (top-level) stays next to `CompactFunc`: it mirrors `slices.Compact` / `slices.CompactFunc`
  - `First` / `Last` now return `(T, error)` with the `ErrEmpty` sentinel (see CONVENTIONS.md)
- [x] document in godoc of `MapCtx`/`FilterCtx`/`TapCtx` that after a fatal error the pool may still
      run the callback on items already buffered (up to `WithParallel(n)` of them) before it shuts down
- [x] export `Suppress(err) error` so a callback can tolerate an error where it has the context to
      decide; the struct stays private, context errors and `ErrPanic` are refused, `CircuitBreaker`
      stays in the root as the operator that defines tolerance (see CONVENTIONS.md)
- [x] `ReduceBy` / `ReduceByCtx`: one fold per key, the map-reduce terminal. Chosen over three
      sugar methods after checking the neighbours (rill has `MapReduce` and nothing else keyed; the
      libraries that have `GroupBy` / `CountBy` / `Find` are in-memory collection toolkits, and their
      helpers work on `Collect()`'s slice anyway):
  - ~~`GroupBy`~~ — `ReduceBy(key, nil, append)`; ~~`CountBy`~~ — `ReduceBy(key, 0, +1)`, and the
    name means "count matches of a predicate" in `lo` but "counts per key" in lodash, Rust and
    Kotlin, so whichever we picked would surprise half the users
  - ~~`Find(p)`~~ — `Filter(p).First()`, same laziness, same short-circuit; Java has no `find`
    either. Shown in `ExampleStream_First_find`
- [x] `CircuitBreaker` is a function for `Through`, not a method: it is an error policy, not an
      operation on values (see CONVENTIONS.md). `Through(CircuitBreaker(5))` does not compile, Go
      cannot infer `T` from the use of a call's result, hence `Through(policy.CircuitBreaker[User](5))`
- [x] extension seam: `Stream.Transform[R](func(iter.Seq2[T, error]) iter.Seq2[R, error])` keeps
      `ctx` and options and, unlike `Seq()`, does not stop at a fatal error. `CircuitBreaker` moved
      to the `policy` sub-package and is written on `Transform` + `Suppress` only, so the seam is
      proven by its first client. The suppressed-error struct lost its counters on the way; the
      breaker wraps its own "failure 2/5" message before calling `Suppress`
- [ ] tag `v0.1.0`

## Phase 4 — Measure the claims

Goal: README claims O(1) memory in intermediate operations; prove it or fix the wording.
Runs against the API frozen in Phase 3 so numbers are not invalidated by signature changes.
`bench_test.go` already holds the first benchmarks plus `memStream`, the removed in-memory type, as
the reference for pure-data overhead.

- [x] first numbers (100k ints, `Map`+`Filter`+`Collect`): plain loop 0.23 ms, `memStream` 0.70 ms,
      `Stream` ~1.6–2.2 ms with identical allocations. The per-callback `defer recover()` cost half
      of `Stream`'s time and was replaced by one guard per stage
- [ ] benchmarks per operator (`Take`, `Skip`, `Chunk`, `Flatten`, `Compact`, `CircuitBreaker`) with
      `-benchmem`; sequential vs `WithParallel(n)` for `MapCtx` / `FilterCtx`; `Merge` fan-in
- [ ] profile the remaining gap to `memStream`: wrapper layers (`Map` → `MapCtx`, `Collect` →
      `ForEach` → `ForEachCtx` → `each`) and `result` copying; decide whether direct implementations of
      the hot plain variants are worth the duplication
- [ ] run on a quiet machine with `-count=10`, summarise with `benchstat`, keep the table in
      `BENCHMARKS.md`
- [ ] adjust README wording to what the benchmarks show
- [ ] optional: CI job that runs benchmarks on PRs and comments the `benchstat` diff

## Parking lot

Ideas without a decision. They enter a phase only with a concrete use case.

- `ZipWith(a, b, fn)` — pairs elements positionally, stops at the shorter stream. Design if picked
  up: `ZipWith` as the only primitive (no `Zip`, no `Pair` in root); `iter.Pull` on the second stream
  with `defer stop()`; when the second stream ends first, the already-pulled element of the first is
  dropped (same as the std `iter.Zip` proposal). Errors: pair the i-th *value* of each side, forward
  errors where they occur; merged context. Tuples, if wanted, go to a `tuple` sub-package (`Pair`,
  `Triple`, `MakePair`, `MakeTriple`) so `ZipWith(a, b, tuple.MakePair)` works without a root
  dependency; no `Zip3With`, show the nested form in an example. Tests must include goleak on
  short-circuit (an unstopped `iter.Pull` leaks a goroutine) and on a parallel stage behind the
  pulled side
- key/value sources, depending on the `tuple` decision above:
  `seq.Pairs(iter.Seq2[K, V]) iter.Seq[tuple.Pair[K, V]]` for any `Seq2` source and
  `seq.KVPairs(map[K]V)` so callers need not import `maps` (iteration order is random — say so).
  ~~`KVStream`~~ was dropped: a dedicated type for a handful of helpers is not worth it
- rate limiting / retry with backoff. `Retry` wraps `MapCtx` (it must re-run the upstream op), so
  it is not an error policy
- **more policies** in `policy`, e.g. an in-stream error observer or a breaker with a time window.
  The seam (`Transform`, `Suppress`) and the package exist; each new policy is additive, hence a
  minor version, and enters with a concrete use case. `Retry` is different: it must re-run the
  upstream operation, so it wraps `MapCtx` rather than transforming the sequence
- a public setter for the read-ahead multiplier of ordered parallel steps (`options.readAhead`,
  default 2). Shape if picked up: a `StepOption` taking a multiplier, not an absolute size, so the
  window can never be smaller than the worker count. Use case to wait for: plenty of memory and
  high latency variance per call, where a wider window keeps workers busy behind a slow item
- an opt-in context-free, error-free stream type for pure in-memory work, if the ~2x overhead ever
  matters to someone; `memStream` in `bench_test.go` is the starting point
- fuzz tests for `Chunk` + `Flatten` round trip
