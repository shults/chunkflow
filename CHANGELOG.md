# Changelog

All notable changes to this module. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

Versioning while the major is 0: a **patch** release changes no exported API (fixes, docs,
benchmarks); a **minor** release adds API or breaks it, and every break is listed under
"Breaking" with what a caller has to change. `0.1.0` freezes the core API: from there on a
break is a deliberate minor bump with a migration note, not a routine.

## [Unreleased] - to be tagged v0.1.0

### Breaking

- The builder's `Seq2` now means what `iter.Seq2` means: any two-value iterator, streamed as
  `Entry[K, V]{Key, Value}` (`New(ctx).Seq2(maps.All(m))`). The fallible constructor, which takes an
  `iter.Seq2[T, error]` and is the inverse of `Stream.Seq()`, is renamed `SeqErr`.
  Migration: replace `.Seq2(` with `.SeqErr(` wherever the iterator's second value is an error;
  the compiler flags every such site.

### Added

- `Entry[K, V]` and `Builder.Seq2` for key/value sources: maps, `slices.All`, any `iter.Seq2[K, V]`.

- `ChunkTimeout[R []T](size, maxWait)`: `Chunk` that also releases a partial chunk once `maxWait`
  has passed since its first value, for sources that trickle. Reads the source on a goroutine.

### Changed

- README rewritten around real use: goals, a `BatchEventSaver` scenario with `Push` / `Stop` /
  `SetOnError` over an injected store (runnable as `Example_batchEventSaver`), then one section per
  concern with examples taken from the tests, and the API tables last.

## [0.0.3] - 2026-09-20

The API polish phase of the roadmap: the shapes that were awkward in `v0.0.2` fixed while nobody
depends on them, the error model completed, and the library measured.

### Breaking

- `New(ctx)` takes no options. Pipeline options go on the stream: `New(ctx).Seq(s).Opts(WithParallel(8))`.
  Migration: move every option from `New(ctx, ...)` to an `Opts(...)` call after the source.
- `First()` and `Last()` return `(T, error)` instead of `(T, bool, error)`. An empty stream is the
  sentinel `ErrEmpty`, checked with `errors.Is`; it never reaches `WithOnError`.
  Migration: replace `v, ok, err := s.First()` with `v, err := s.First()` and test
  `errors.Is(err, ErrEmpty)` where `!ok` was tested.
- `Exec()` is renamed `Drain()`.
- `WithParallel(n)` keeps source order by default. `MapCtx`, `FilterCtx` and `TapCtx` on `n > 1`
  workers now emit results in the order of the source, behind a read-ahead window of `2n`
  elements. Code that relied on arrival order gets it back with `WithUnordered()`; code that
  sorted after a parallel stage can stop.
- `CircuitBreaker` is no longer a method. It lives in the `policy` sub-package as a function for
  `Through`, and the element type must be spelled out because Go cannot infer it from the
  threshold: `s.CircuitBreaker(5)` becomes `s.Through(policy.CircuitBreaker[User](5))`.
- The message of a tolerated error changed from `suppressed by circuit breaker (failure 1/5): boom`
  to `suppressed: circuit breaker failure 1/5: boom`. `errors.Is(err, ErrSuppressed)` and the
  original error in the chain are unchanged.

### Added

- `Suppress(err) error` marks an error as tolerated from inside any `*Ctx` callback: the element
  is skipped, terminals go on, `WithOnError` and `Seq()` still see it. Refuses context errors and
  `ErrPanic`. It is also the only way a policy marks an error.
- `WithUnordered()` step option: a parallel stage emits results as they arrive.
- `ReduceBy[K, R](key, init, fn)` and `ReduceByCtx`: one fold per key, the map-reduce terminal
  (grouping, counting, sums by key).
- `Transform[R](func(iter.Seq2[T, error]) iter.Seq2[R, error]) Stream[R]`: the extension seam.
  Unlike `Seq()` it does not stop at a fatal error and it keeps the context and options.
- `Zip[O, R](other, fn)` and `ZipCtx`: pair two streams position by position through a callback;
  ends at the shorter side, errors pass at their position, contexts merged like `Merge`.
- Sub-package `policy`: `CircuitBreaker[T](n)`, written on `Transform` and `Suppress` only, the
  way a third-party policy would be.
- Sub-package `step`: decorators for the callbacks themselves. `Decorate(fn, mws...)` for
  `func(ctx, T) (R, error)`, `Decorate3` for fold callbacks, one `Middleware` type serving both;
  `Retry(attempts, RetryWithBackoff(newBackoff))` over a `Backoff` interface with the method set
  of `cenkalti/backoff` (no dependency taken), `Timeout(d)`, `Tolerate(targets...)`.
- `ErrEmpty` sentinel.
- Benchmarks per operator, pool and simulated I/O in `bench_ops_test.go`, results and their
  reading in `BENCHMARK.md`, `make bench`, and a CI job that compares a pull request against its
  base with `benchstat`.
- Pull request template.

### Changed

- Godoc of `MapCtx`, `FilterCtx` and `TapCtx` documents that after a fatal error the pool may
  still run the callback on up to `n` buffered items before it shuts down.
- README rewritten around the one-type design, a "Performance" section with the measured rule
  of thumb (a stage ~10 ns/element, a pool ~0.5 µs/element, break-even around a few µs of work).

### Removed

- The internal `memStream` type is gone from the library; it survives in `bench_test.go` as the
  reference for what the error/context plumbing costs.

## [0.0.2] - 2026-09-16

One stream type (`Stream`, formerly `IoStream`), the `New(ctx, opts...)` builder with `Seq`,
`Seq2`, `Chan`, errors as elements with `CircuitBreaker` and `ErrSuppressed`, panics as `ErrPanic`,
`*Ctx` naming, `WithParallel`, `WithOnError`, the `seq` generators, CI with race tests, lint,
`govulncheck` and 100% statement coverage.

## [0.0.1] - 2026-09-15

First tagged version.

[Unreleased]: https://github.com/shults/chunkflow/compare/v0.0.3...HEAD
[0.0.3]: https://github.com/shults/chunkflow/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/shults/chunkflow/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/shults/chunkflow/releases/tag/v0.0.1
