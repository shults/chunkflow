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
  - [x] scenario: consumer short-circuits (`Take(1)`) after `MapAsync(WithParallel(4))` on `seq.Numbers`
  - [x] scenario: context cancelled while a worker is blocked on the output channel
  - [x] scenario: one worker fails while the others are still running
  - [x] scenario: upstream source yields an error while workers are busy
- [x] **Close coverage gaps** (statement coverage is now 100%)
  - [x] `IoStream.CircuitBreaker` — trips at threshold, resets on success, rejects threshold < 1
        (fixed on the way: intermediate stages now forward errors instead of ending the stream, so the
        breaker works anywhere in the chain; tolerated errors are re-emitted marked as `ErrSuppressed` and
        skipped by terminals, observable via `Seq()` + `errors.Is(err, ErrSuppressed)`)
  - [x] `IoStream.Exec`, `IoStream.Reduce`
  - [x] error paths of `IoStream.Chunk`, `IoFlatten`, `AllAsync`, `AnyAsync`, `ReduceAsync`
  - [x] `WithLogger` — assert the `ReduceAsync` concurrency warning is emitted
  - [x] `Stream.All` / `Stream.Any` on an empty stream
- [x] **Example tests** (`example_test.go`) for `Stream`, `IoStream`, `seq` — they double as godoc
- [x] **Package docs**: `doc.go` in the root package with the overview currently only in README

## Phase 2 — Measure the claims

Goal: README states "O(1) memory" and "no heap allocation in intermediate ops"; prove or fix the wording.

- [ ] `BenchmarkStream_*` for `Map`, `Filter`, `Take`, `Skip`, `Chunk`, `Flatten` with `-benchmem`
- [ ] `BenchmarkIoStream_*` for the sequential and the `WithParallel(n)` paths of `MapAsync` / `FilterAsync`
- [ ] baseline the numbers with `benchstat` and keep the results in `BENCHMARKS.md`
- [ ] adjust README wording to what the benchmarks show (per-stage vs per-element allocations)
- [ ] optional: CI job that runs benchmarks on PRs and comments the `benchstat` diff

## Phase 3 — API polish (breaking, do before tagging v0.1.0)

Goal: fix the shapes that are awkward now, while nobody depends on them.

- [ ] `Reduce(init, fn(acc, item))` — flip the accumulator to Go's conventional order on both stream types
- [ ] recover panics inside worker goroutines and surface them as errors (a panic in a worker currently kills the process)
- [ ] `WithOrdered()` option for `MapAsync` / `FilterAsync` — preserve input order under `WithParallel(n > 1)`
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

## Phase 4 — Operator set

Goal: fill in the operators a typical ETL pipeline needs, keeping `Stream` / `IoStream` symmetric.

- [ ] `TakeWhile`, `SkipWhile`
- [ ] `Tap(fn func(T))` — per-element side effect that passes the element through unchanged
      (sugar for `Map(func(x T) T { fn(x); return x })`; not to be confused with `Through`,
      which rewires the whole pipeline). `TapAsync` on `IoStream`.
- [x] `Count()` terminal
- [ ] `Distinct()` for comparable `T` (top-level function + `Through`, like `Flatten`)
- [ ] `Concat(a, b, ...)` — sequential: drains `a`, then emits `b`; deterministic order
- [ ] `Merge(a, b, ...)` on `IoStream` only — concurrent fan-in, interleaved order
- [ ] `Zip(a, b)` — pairs elements positionally, stops at the shorter stream
- [ ] `seq`: `FromChan`, `Repeat(val, n)`, `Iterate(seed, fn)`

## Parking lot

Ideas without a decision yet.

- `Stream.Io(ctx)` shortcut instead of `NewIoStream(ctx, s.Seq())`
- rate limiting / retry with backoff as `IoStream` operators
- `iter.Seq2[K, V]`-based `KVStream`
- fuzz tests for `Chunk` + `Flatten` round trip
