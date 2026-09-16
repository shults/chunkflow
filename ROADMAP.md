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

- [ ] `New(ctx)` without options. `New(ctx, opts...)` and `Stream.Opts(opts...)` are two ways to do
      one thing; the source methods (`Seq`, `Seq2`, `Chan`) use no options, so nothing is lost by
      `New(ctx).Chan(ch).Opts(WithParallel(8))`. The builder then holds only the context
- [ ] `WithOrdered()` option for `MapCtx` / `FilterCtx` / `TapCtx` — preserve input order under
      `WithParallel(n > 1)`
  - sliding window: `n` workers pull freely, but results are emitted strictly in source order;
    a finished item whose predecessors are still running waits in a reorder buffer
  - bounded read-ahead via `WithWindow(k)` (default `k = 2n`): at most `k` items may be pulled from
    the source and not yet emitted; when the window is full, idle workers wait instead of pulling.
    Keeps memory O(k) even when the head-of-line item is slow
  - implementation sketch: the feeder numbers items and creates a one-shot result channel per item,
    pushing those channels in order into a queue of capacity `k` (this is the backpressure); workers
    take `(item, resultChan)` from the input channel and fill it; the emitter reads the queue in order
    and blocks on each channel
  - errors are ordinary results at their own position, so a downstream `CircuitBreaker` sees failures
    in source order rather than arrival order
  - cost to document: head-of-line blocking — one slow item stalls emission (and, once the window is
    full, the workers) behind it; that is the price of ordering, hence opt-in, never default
  - tests: order preserved under random per-item delays; window bound respected (source pulls never
    exceed emitted + k); leak-free on short-circuit and cancellation (goleak); `Take(1)` after an
    ordered stage stops the pool
- [ ] API trim review before the freeze — every exported name must defend its place. Candidates:
  - `Count()` — one-line `Reduce`
  - `Exec()` — `ForEach` with an empty func
  - `Compact` (top-level) next to `CompactFunc` — saves one lambda
  - `First` / `Last` returning `(T, bool, error)` vs `(T, error)` with an `ErrEmpty` sentinel
- [ ] document in godoc of `MapCtx`/`FilterCtx`/`TapCtx` that after a fatal error the pool may still
      run the callback on items already buffered (up to `WithParallel(n)` of them) before it shuts down
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
- **error policies as plug-ins** (gut says "not yet"). `CircuitBreaker` is one instance of a stage
  that looks only at errored elements and decides *fatal* / *tolerated* / *replaced*. The extension
  seam already exists (errors are elements, `Seq()` / `New(ctx).Seq2` cross the boundary both ways,
  `Through` plugs a transform in); the only missing piece would be an exported `Suppress(err) error`,
  never the struct. Rule of three: the *second* policy does not enter as another method — at that
  point either generalise to `OnError(policy)` + `Suppress`, or accept two concrete operators. If it
  happens, `CircuitBreaker` moves to a neighbouring sub-package so the core stays `Stream`, the
  builder and the terminals
- an opt-in context-free, error-free stream type for pure in-memory work, if the ~2x overhead ever
  matters to someone; `memStream` in `bench_test.go` is the starting point
- fuzz tests for `Chunk` + `Flatten` round trip
