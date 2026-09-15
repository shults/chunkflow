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
- [ ] **Goroutine leak detection** with `go.uber.org/goleak`
  - [ ] `goleak.VerifyTestMain(m)` in a `TestMain` for the root package
  - [ ] `defer goleak.VerifyNone(t)` in every test that exercises `WithParallel(n > 1)`
  - [ ] scenario: consumer short-circuits (`Take(1)`) after `MapAsync(WithParallel(4))` on `seq.Numbers`
  - [ ] scenario: context cancelled while a worker is blocked on the output channel
  - [ ] scenario: one worker fails while the others are still running
  - [ ] scenario: upstream source yields an error while workers are busy
- [ ] **Close coverage gaps** (currently untested)
  - [ ] `IoStream.CircuitBreaker` — trips at threshold, resets on success, rejects threshold < 1
  - [ ] `IoStream.Exec`, `IoStream.Reduce`
  - [ ] error paths of `IoStream.Chunk`, `IoFlatten`, `AllAsync`, `AnyAsync`, `ReduceAsync`
  - [ ] `WithLogger` — assert the `ReduceAsync` concurrency warning is emitted
  - [ ] `Stream.All` / `Stream.Any` on an empty stream
- [ ] **Example tests** (`example_test.go`) for `Stream`, `IoStream`, `seq` — they double as godoc
- [ ] **Package docs**: `doc.go` in the root package with the overview currently only in README

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
- [ ] decide the fate of `WithLogger`: remove it, or give it real work (worker start/stop, circuit breaker trips)
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
