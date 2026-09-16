# Roadmap

Working TODO list for ChunkFlow. Items are grouped into phases; a phase should be
finished (or consciously deferred) before the next one starts. Tick items off as
they land on `master`.

## Phase 1 — Safety net (no API changes)

Goal: every future change is guarded by CI, leak detection and coverage.

- [x] **CI workflow** (`.github/workflows/ci.yml`)
  - [x] `gofmt -l` check (fail on unformatted files)
  - [x] `go vet ./...`
  - [x] `go test -race -shuffle=on -cover ./...`
  - [x] `golangci-lint` with a small, explicit config (`.golangci.yml`)
  - [x] `govulncheck ./...`
  - [x] matrix: Go `1.27.x` and `stable`
- [x] **Local tooling**: `Makefile` (`setup`, `check`, `test`, `ci`) and a versioned pre-commit hook in `.githooks/`
      running gofmt, go vet, golangci-lint and go mod tidy
- [x] **Goroutine leak detection** with `go.uber.org/goleak`
  - [x] `goleak.VerifyTestMain(m)` in a `TestMain` for the root package
  - [x] `defer goleak.VerifyNone(t)` in every test that exercises `WithParallel(n > 1)`
  - [x] scenario: consumer short-circuits (`Take(1)`) after `MapCtx(WithParallel(4))` on `seq.Numbers`
  - [x] scenario: context cancelled while a worker is blocked on the output channel
  - [x] scenario: one worker fails while the others are still running
  - [x] scenario: upstream source yields an error while workers are busy
- [x] **Close coverage gaps** (statement coverage is now 100%)
  - [x] `IoStream.CircuitBreaker` — trips at threshold, resets on success, rejects threshold < 1
        (fixed on the way: intermediate stages now forward errors instead of ending the stream, so the
        breaker works anywhere in the chain; tolerated errors are re-emitted marked as `ErrSuppressed` and
        skipped by terminals, observable via `Seq()` + `errors.Is(err, ErrSuppressed)`)
  - [x] `IoStream.Exec`, `IoStream.Reduce`
  - [x] error paths of `IoStream.Chunk`, `IoFlatten`, `AllCtx`, `AnyCtx`, `ReduceCtx`
  - [x] `WithLogger` — assert the `ReduceCtx` concurrency warning is emitted
  - [x] `Stream.All` / `Stream.Any` on an empty stream
- [x] **Example tests** (`example_test.go`) for `Stream`, `IoStream`, `seq` — they double as godoc
- [x] **Package docs**: `doc.go` in the root package with the overview currently only in README

## Phase 2 — Operator set

Goal: fill in the operators a typical ETL pipeline needs, keeping `Stream` / `IoStream` symmetric.

- [x] `TakeWhile`, `SkipWhile` (+ `TakeWhileCtx`, `SkipWhileCtx` on `IoStream`)
- [x] `Tap(fn func(T))` — per-element side effect that passes the element through unchanged
      (sugar for `Map(func(x T) T { fn(x); return x })`; not to be confused with `Through`,
      which rewires the whole pipeline). `TapCtx` on `IoStream`.
- [x] `Count()` terminal
- [x] ~~`Distinct()`~~ — deliberately not added. A global "seen" set needs O(unique) memory and
      belongs to the user (DB, KV store, Bloom filter), not inside the pipeline. The batched
      pattern `Chunk(n).MapCtx(dedupAgainstStore).Through(IoFlatten)` is documented as
      `ExampleIoStream_Chunk_deduplication`; one round trip per batch beats a per-element check
- [x] `Compact()` / `CompactFunc(eq)` — drop *consecutive* duplicates in O(1) memory, same contract
      and name as `slices.Compact`; precondition (sorted or grouped input) stated in the first
      sentence of the godoc. `Compact` needs `comparable`, so top-level + `Through`; `CompactFunc`
      as a method
- [x] `Concat(streams ...)` / `IoConcat` — sequential: drains the first, then the next; deterministic order.
      Top-level function on both `Stream` and `IoStream`. Name matches `slices.Concat`
- [x] `IoMerge(streams ...)` on `IoStream` only — concurrent fan-in, interleaved order.
      Same variadic top-level shape as `Concat`; the single word is the only difference.
      Decided against `MergeOrdered`/`MergeUnordered`: "ordered merge" reads as merge-sort of
      sorted inputs, and the two operations are not symmetric (`Merge` needs goroutines, so no
      `Stream` variant)
- [x] channels: `NewIo(ctx, opts...)` builder with `Seq`, `Seq2`, `Chan` replaced `NewIoStream`/`NewIoStream2`.
      `Chan` lives on the builder rather than in `seq` because it needs the context to unblock a
      receive; the builder also binds default options once
- [x] `seq`: `Repeat(val, n)`, `Iterate(seed, fn)`

## Phase 3 — API polish (breaking, do before tagging v0.1.0)

Goal: fix the shapes that are awkward now, while nobody depends on them.

- [x] rename the `*Async` family to `*Ctx`: `MapCtx`, `FilterCtx`, `TapCtx`, `TakeWhileCtx`, `SkipWhileCtx`,
      `ForEachCtx`, `ReduceCtx`, `AllCtx`, `AnyCtx`. `Async` describes behaviour these methods do not
      have (they block; parallelism is a separate `WithParallel` option). What actually differs is the
      callback shape: it receives a `context.Context` and may return an error — the Go std convention
      for that is a context suffix (`ExecContext`, `DialContext`); `Ctx` is the short form. A suffix,
      not a prefix, so `Map` and `MapCtx` sit next to each other in godoc and completion. Mechanical
      rename across code, tests, examples, README, doc.go, AGENTS.md. Conventions and their
      reasons are now recorded in `CONVENTIONS.md`
- [x] `Reduce[R](init R, fn(acc R, item T) R)` — callback order flipped to Go's conventional
      `(acc, item)` on both stream types and the accumulator got its own type parameter. `init` stays
      the first parameter, before the closure: an init value trailing a multi-line func literal reads badly
- [ ] recover panics inside worker goroutines and surface them as errors (a panic in a worker currently kills the process)
- [ ] `WithOrdered()` option for `MapCtx` / `FilterCtx` — preserve input order under `WithParallel(n > 1)`
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
- [ ] decide the fate of `WithLogger`: remove it, or give it real work (worker start/stop, breaker trips)
- [ ] document (or unify) `Stream.Chunk` panicking vs `IoStream.Chunk` emitting an error on invalid size
- [ ] tag `v0.1.0`

## Phase 4 — Measure the claims

Goal: README states "O(1) memory" and "no heap allocation in intermediate ops"; prove or fix the wording.
Runs last, against the API frozen in Phase 3, so numbers are not invalidated by signature changes.

- [ ] `BenchmarkStream_*` for `Map`, `Filter`, `Take`, `Skip`, `Chunk`, `Flatten` with `-benchmem`
- [ ] `BenchmarkIoStream_*` for the sequential and the `WithParallel(n)` paths of `MapCtx` / `FilterCtx`
- [ ] baseline the numbers with `benchstat` and keep the results in `BENCHMARKS.md`
- [ ] adjust README wording to what the benchmarks show (per-stage vs per-element allocations)
- [ ] optional: CI job that runs benchmarks on PRs and comments the `benchstat` diff

## Parking lot

Ideas without a decision yet.

- `Stream.Io(ctx)` shortcut instead of `NewIo(ctx).Seq(s.Seq())`
- `ZipWith(a, b, fn)` / `IoZipWith` — pairs elements positionally, stops at the shorter stream.
  Backlog, no committed use case yet. Design if picked up: `ZipWith` as the only primitive (no `Zip`,
  no `Pair` in root); `iter.Pull` on the second stream with `defer stop()`; when the second
  stream ends first, the already-pulled element of the first is dropped (same as the std
  `iter.Zip` proposal). Errors: pair the i-th *value* of each side, forward errors where they
  occur; merged context. Tuples, if wanted, go to a `tuple` sub-package (`Pair`, `Triple`,
  `MakePair`, `MakeTriple`) so `ZipWith(a, b, tuple.MakePair)` works without a root
  dependency; no `Zip3With`, show the nested form in an example instead. Tests must include
  goleak on short-circuit (an unstopped `iter.Pull` leaks a goroutine) and on a parallel stage
  behind the pulled side
- rate limiting / retry with backoff as `IoStream` operators
- ~~`iter.Seq2[K, V]`-based `KVStream`~~ — dropped: it would need both a sync and an Io variant
  (four stream types instead of two) for a handful of key/value helpers. Same problem, smaller
  cost: pair elements. Depends on the `tuple` sub-package decided together with `ZipWith`:
  - `seq.Pairs(iter.Seq2[K, V]) iter.Seq[tuple.Pair[K, V]]` — adapter for any `Seq2` source
  - `seq.KVPairs(map[K]V) iter.Seq[tuple.Pair[K, V]]` — map shortcut so callers need not import `maps`
    (`maps.Keys` / `maps.Values` already plug straight into `NewStream` / `NewIo(ctx).Seq`)
  - key/value helpers, if ever needed, as top-level functions via `Through`, never a new stream type
- fuzz tests for `Chunk` + `Flatten` round trip
- **error policies as plug-ins** (undecided, gut says "not yet"). `CircuitBreaker` is one instance of a
  more general thing: a stage that looks only at errored elements and decides *fatal* / *tolerated* /
  *replaced*. Others of the same family: tolerate all, tolerate `errors.Is(err, X)`, tolerate below an
  error rate in a window. `Retry` is not one of them (it must re-run the upstream op, so it wraps
  `MapCtx` instead).
  - the extension seam already exists: errors flow as elements, `Seq()` / `NewIo(ctx).Seq2` cross the
    boundary in both directions, `Through` plugs a transform in. The only missing piece is a way to
    mark an error as tolerated from outside — an exported constructor such as `Suppress(err) error`,
    never the struct or its fields
  - rule of three: do not generalise from one case. The *second* policy does not enter as another
    method; at that point either add `OnError(policy)` + `Suppress`, or accept two concrete operators
  - if that happens, `CircuitBreaker` (and possibly `Flatten`/`Compact`-style top-level helpers) move
    out of the core stream types into a neighbouring place — a `policy`/`ops` sub-package — so the core
    stays the two stream types, the builder and the terminals
