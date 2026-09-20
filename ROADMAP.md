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
- [ ] tag `v0.0.3` (with `CHANGELOG.md`). Not `v0.1.0` yet: the roadmap meant `v0.1.0` as the freeze,
      and `Transform`, `policy`, `step` and `Zip` are days old with no user but this repository.
      `v0.1.0` comes when the core API has survived real use and no breaking change is queued;
      until then additions and breaks are `v0.0.x`, see the versioning note in CHANGELOG.md

## Phase 4 — Measure the claims

Goal: README claims O(1) memory in intermediate operations; prove it or fix the wording.
Runs against the API frozen in Phase 3 so numbers are not invalidated by signature changes.
`bench_test.go` already holds the first benchmarks plus `memStream`, the removed in-memory type, as
the reference for pure-data overhead.

- [x] first numbers (100k ints, `Map`+`Filter`+`Collect`): plain loop 0.23 ms, `memStream` 0.70 ms,
      `Stream` ~1.6–2.2 ms with identical allocations. The per-callback `defer recover()` cost half
      of `Stream`'s time and was replaced by one guard per stage
- [x] benchmarks per operator (`Take`, `Skip`, `Chunk`, `Flatten`, `Compact`, `CircuitBreaker`) with
      `-benchmem`; sequential vs `WithParallel(n)` for `MapCtx` / `FilterCtx`; `Merge` fan-in. Added on
      the way: ordered vs `WithUnordered()` pools on CPU-trivial and on simulated I/O (uniform and
      skewed latency), `Transform`, `Suppress`, `ReduceBy`, `step` decorators. `bench_ops_test.go`
- [x] profile the remaining gap to `memStream`: no hot spot, the cost is the depth of nested
      `yield` calls. The convenience adapters are ~30% of flat time, the source's `ctx.Err()` +
      `result` construction ~22%. Decision: **not** duplicating `Map`/`Filter`/`Collect` for a
      quarter of a 12 ns/element gap; revisit only if someone measures it in a real pipeline
- [x] `-count=8` (laptop, `powersave` governor, desktop running; GC-heavy cases ±25%, the rest
      ±1–8%), `benchstat` via `make bench`, tables and interpretation in `BENCHMARK.md`. A quiet
      machine would tighten the intervals, not move the medians
- [x] README got a "Performance" section with the rule of thumb (10 ns/element per stage, ~0.5 µs
      per element for a pool, 7.9× on eight workers with order kept); the "bounded" claim is
      confirmed by constant allocation counts
- [x] CI: `.github/workflows/bench.yml` benchmarks the PR head and its base (`count=6`) and writes
      the `benchstat` comparison to the job summary, raw outputs as artifacts. Advisory, never
      failing: hosted runners are too noisy for thresholds, `make bench` locally is the arbiter

## Phase 5 — additive extensions (minor versions)

Everything here adds API without changing existing signatures, so it does not wait for a freeze.

- [x] `Zip[O, R](other, fn)` / `ZipCtx` as methods: pairs values positionally through a callback,
      ends at the shorter side, errors pass at their position, merged context, `iter.Pull` on the
      other side (the library's first coroutine; goleak on short-circuit, cancellation and a
      parallel stage behind the pulled side). No `Zip3`: nest and extend a struct in the callback
- [x] `ChunkTimeout[R []T](size, maxWait)`: `Chunk` with a bound on how long a value waits for its
      chunk, the batching operator for trickling sources (rill's `Batch`). Own name rather than an
      option on `Chunk` because it needs a feeder goroutine (see CONVENTIONS.md)

## Parking lot

Ideas without a decision. They enter a phase only with a concrete use case.

- ~~`ZipWith(a, b, fn)`~~ — shipped as the method `Zip(other, fn)` (Phase 5). ~~`tuple` sub-package
  (`Pair`, `Triple`, `MakePair`)~~ — dropped: Go cannot grow a struct per zip level, so a generic pair
  becomes `p.First.First.Second` at three sources while the callback names the fields; and a
  `Pair` in the root API would be a one-way door for a type with no behaviour
- key/value sources: `maps.All(m)` is an `iter.Seq2[K, V]`, but the builder's `Seq2` reads a
  `Seq2` as `(value, error)`, so a map needs a five-line adapter to a `struct{K; V}` sequence. With
  `tuple` dropped, a `seq.Entries(m)` would export an `Entry[K, V]` type; waits for someone to ask,
  an example of the adapter is the cheaper answer. ~~`KVStream`~~ was dropped: a dedicated type for
  a handful of helpers is not worth it
- **step decorators**: prototyped in `step` as `Decorate(fn, mws...)` / `Decorate3(fn, mws...)`
  over `Middleware func(ctx, call func(ctx) error) error`, with `Retry(attempts, opts...)`,
  `Timeout(d)`, `Tolerate(targets...)`; 100% covered. `Retry` is the middleware and the pacing is
  its injected strategy: `RetryWithBackoff(newBackoff)` over the `Backoff` interface with the
  method set of `cenkalti/backoff` (`NextBackOff() time.Duration`, `Reset()`, `Stop = -1`), so
  its strategies or anyone else's plug in unchanged and the module takes no dependency. No stock
  strategies of our own: `Constant` and `Exponential` were written and removed, pacing is a solved
  problem elsewhere and every exported name must defend its place. `NextBackOff` is stateful, so the option takes a
  constructor, `func() Backoff` (a concrete constructor such as `backoff.NewExponentialBackOff` is
  wrapped in a one-line closure; a generic option was tried and dropped as not worth a type
  parameter), and every series of attempts, i.e. every element, gets a fresh, reset instance:
  nothing is shared between workers. Whether it
  ships in this module or moves to its own is open. Findings that changed the earlier notes:
  - the list shape *does* compile once middlewares are non-generic: a middleware sees only the
    context and the error, the values stay in the decorator's closure, so `Retry(3, nil)` needs no
    type arguments and `Decorate` infers `T, R` from `fn`. Reflection was considered for arity
    erasure and rejected: closures do it at zero cost and keep compile-time checking
  - arity is handled by one decorator per callback shape (`Decorate`, `Decorate3`), not by adapters;
    the middlewares are shared. A third shape (`func(ctx, T) error`) is ten more lines when needed
  - "skip this element" is a suppressed error coming out of the chain; the decorator knows what
    that means for its shape (`Decorate`: return the marked error; `Decorate3`: return the
    accumulator unchanged and no error, because a terminal treats any callback error as fatal)
  - a retried fold callback gets the same accumulator every attempt; a reference-type accumulator
    may carry the failed attempt's mutations, so mutate after the fallible work
  - "never retry context errors" is wrong as stated: an inner `Timeout` yields `DeadlineExceeded`
    that *should* be retried; the rule is "never retry once the *caller's* context is done"
  Original findings follow. The second extension family next to policies, see CONVENTIONS.md. A decorator wraps the callback itself,
  `func(ctx, T) (R, error)` in and out, so it can re-run or bound a call; a policy on the stream
  cannot. It needs nothing from the root, the callback signature is plain Go, so it is its own
  package (`step`, not `middlewares`: singular, short, and "middleware" promises request/response
  access it does not have), inside or outside the module, and always a minor. Parked until the
  first real user, with these findings so the design is not redone:
  - shape, checked with the compiler: `Decorate(fn, Retry(3), Timeout(d))` does not compile, Go
    cannot infer `Retry`'s `T, R` from where its result goes, and `Retry[User, Row](3)` on every
    element is worse than the problem. Three shapes do compile without type arguments:
    nested `step.Retry(3, step.Timeout(d, fetch))` (no exported type, reads outside-in, awkward
    past three layers); a list of generic method values `Decorate(fetch, step.Retry(3).Apply, ...)`
    (`.Apply` noise, exports the config type); a builder
    `step.Wrap(fetch).Timeout(d).Retry(3).Fn()` (reads like the stream, exports one generic type,
    last call is outermost). Preferred: nested first, builder added beside it only if three
    layers in one place turn out to be common; both work on the bare callback, so nothing is lost
  - arity: the callback shapes in the root are `func(ctx, T) (R, error)` (`MapCtx`, and with
    `R = bool` also `FilterCtx`, `TakeWhileCtx`, `SkipWhileCtx`, `AllCtx`, `AnyCtx`),
    `func(ctx, T) error` (`TapCtx`, `ForEachCtx`) and `func(ctx, acc R, T) (R, error)` (`ReduceCtx`,
    `ReduceByCtx`). One decorator set cannot cover all three without adapters or three copies.
    Decision for a first version: target the first shape only, where retry and timeouts actually
    happen (I/O in `MapCtx`, predicates that call out); a side effect that needs retrying is a
    `MapCtx` returning its input, which is what `TapCtx` is internally anyway; folds are in-memory
    and get nothing. Adapters (`step.ForTap`, `step.ForFold`) only with a use case
  - semantics to fix before writing `Retry`: never retry context errors or errors already marked
    by `Suppress` (someone has decided), retries are per element and invisible to a downstream
    breaker, a panic in the callback is caught by the stage guard above the decorator and must not
    be retried. `SuppressErrors(context.Canceled)` would be a no-op, `Suppress` refuses context
    errors; the realistic target is a domain error such as `ErrNotFound`
  - composition with policies is the point: `MapCtx(step.Retry(3, fetch), WithParallel(8)).
    Through(policy.CircuitBreaker[User](5))` is three attempts per element, then a breaker over
    five elements in a row; neither mechanism can express the other
- **more policies** in `policy`, e.g. an in-stream error observer or a breaker with a time window.
  The seam (`Transform`, `Suppress`) and the package exist; each new policy is additive, hence a
  minor version, and enters with a concrete use case. `Retry` is different: it must re-run the
  upstream operation, so it wraps `MapCtx` rather than transforming the sequence
- **two optimisations the benchmarks pointed at**, neither taken yet: `policy.CircuitBreaker`
  formats a message with `fmt.Errorf` per tolerated error (~430 ns and 5 allocations vs ~125 ns
  and 2 for a bare `Suppress`); a lazy error type would remove most of it. The ordered pool
  allocates a ticket and a result channel per element (2 allocations, ~20% over unordered); a ring
  buffer of size `k` indexed by sequence number would allocate once per stage. Beyond that, the
  remaining ~500 ns/element is six channel operations per element; only handing over *batches*
  (double buffering: two slices swapped between feeder and workers, tickets per batch) amortises
  them, at the price of a window measured in batches, an idle-flush for slow sources, and
  batch-granular cancellation. It speeds up exactly the workload that should not use a pool
  (sub-microsecond callbacks) and changes nothing for I/O, so it waits for someone measuring
  the pool as the bottleneck of a real pipeline
- a public setter for the read-ahead multiplier of ordered parallel steps (`options.readAhead`,
  default 2). Shape if picked up: a `StepOption` taking a multiplier, not an absolute size, so the
  window can never be smaller than the worker count. Use case to wait for: plenty of memory and
  high latency variance per call, where a wider window keeps workers busy behind a slow item
- an opt-in context-free, error-free stream type for pure in-memory work, if the ~2x overhead ever
  matters to someone; `memStream` in `bench_test.go` is the starting point
- fuzz tests for `Chunk` + `Flatten` round trip
