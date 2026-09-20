# Benchmarks

What the abstraction costs, measured rather than claimed. Numbers are medians of 8 runs
(`make bench`, `go test -bench . -benchmem -count=8`, summarised with `benchstat`), taken on
2026-09-20 on a laptop:

| | |
| --- | --- |
| CPU | 11th Gen Intel Core i7-11850H @ 2.50GHz, 8 cores / 16 threads, `powersave` governor |
| Go | go1.27.1 linux/amd64, kernel 7.0 |
| Load | a desktop session was running; GC-heavy cases show ±20–25% spread, the rest ±1–8% |

Every pipeline processes `benchN = 100 000` ints unless stated otherwise. Per-element figures
below are `sec/op ÷ 100 000`. Raw `benchstat` output is at the end; `bench_test.go` and
`bench_ops_test.go` hold the code.

## 1. What a stage costs

Sequential, CPU-trivial callbacks, so the number is the plumbing, not the work.

| Pipeline | ns / element | allocs / run | Notes |
| --- | ---: | ---: | --- |
| plain `for` loop (baseline) | 3.4 | 23 | `Map`+`Filter`+append by hand |
| `memStream` `Map`·`Filter`·`Collect` | 7.2 | 30 | the removed context-free type, kept in `bench_test.go` |
| `Stream` `Map`·`Filter`·`Collect` | 19.8 | 45 | **2.7× memStream, 5.9× the loop** |
| `Stream` `Chunk(100)`·`Flatten`·`Reduce` | 21.5 | 1 017 | memStream: 7.7 ns; allocations are the chunks |
| `Map`·`Drain` (identity) | 10.3 | 13 | the floor for one stage plus source and terminal |
| `Take(n)` from an infinite source | 9.0 | 11 | |
| `Skip(n)` of 2n | 7.2 per pulled element | 11 | |
| `Chunk(100)` | 7.5 | 1 012 | one allocation per chunk, nothing else |
| `Chunk(100)`·`Flatten` | 12.0 | 1 015 | |
| `Compact` (runs of 10) | 11.5 | 19 | |
| `Transform` (identity) | 10.3 | 13 | **the extension seam is free**: same as `Map` |
| `ReduceBy` (10 keys) | 23.9 | 13 | map lookup and store per element |
| `Zip` (two 100k sources) | 89.9 | 20 | measured 2026-09-20 after the first table; the other side is driven by `iter.Pull`, and its coroutine switch is ~80 ns per element, nine times a plain stage. Allocations stay constant |

Allocations are constant in the stream length: 11–20 for 100 000 elements, regardless of the
operator. That is the "bounded" claim in the README, measured. It also means that any allocation
count that grows with the stream comes from the callbacks, not the pipeline: results travel by
value, so `return Row{...}` allocates nothing per element, while `return &Row{...}` or returning a
struct through an interface allocates once per element, in `Zip` as in `Map`. Sometimes that is
the right call, a large struct mutated by later stages is cheaper to allocate once than to copy
four times; it is a decision about the data, not about the pipeline. The 2.7× against `memStream` is
about 12 ns per element per stage in absolute terms; any I/O callback is three to five orders of
magnitude above that.

**Where the 2.7× goes** (CPU profile of `Stream_MapFilterCollect`): nowhere in particular. There
is no hot spot, the cost is the depth of the iterator stack. An element passes through nested
`yield` calls of `Range → Seq (ctx check, result struct) → MapCtx → Map's adapter → FilterCtx →
Filter's adapter → each → ForEachCtx → ForEach → Collect`; `memStream` has four of those. The
convenience adapters (`Map` over `MapCtx`, `Filter` over `FilterCtx`, `Collect` over `ForEach`
over `ForEachCtx` over `each`) are about 30% of the flat time, the source's per-element
`ctx.Err()` plus `result` construction another 22%. Direct implementations of the plain variants
would buy roughly a quarter of the gap at the price of four duplicated operators; not taken, see
ROADMAP.md.

## 2. Tolerating errors

Every tenth element fails; nothing trips. Cost is per element, so per error multiply by ten.

| Pipeline | ns / element | allocs / run | Notes |
| --- | ---: | ---: | --- |
| `MapCtx` returning `Suppress(err)` | 22.0 | 20 010 | ~125 ns and 2 allocations per suppressed error |
| `MapCtx` · `policy.CircuitBreaker` | 64.8 | 50 020 | ~430 ns and 5 allocations per tolerated error |

The breaker is three times the plain `Suppress` path per error because it formats a
`"circuit breaker failure %d/%d"` message with `fmt.Errorf` for every tolerated error before
wrapping it. A lazy error type would remove most of that; noted as a candidate, not done, since
at 10% failures it is still under 70 ns per element.

## 3. Worker pools

### CPU-trivial work: the pool's own overhead

Identity callback, `WithParallel(4)`. The sequential variant is expected to win; this measures
what a parallel stage costs before the callback does anything.

| Pipeline | ns / element | allocs / run | Notes |
| --- | ---: | ---: | --- |
| `MapCtx` sequential | 9.5 | 12 | |
| `MapCtx` `WithParallel(4)`, ordered (default) | 572 | 200 000 | 2 allocations per element: the ticket and its result channel |
| `MapCtx` `WithParallel(4)`, `WithUnordered()` | 478 | 24 | |
| `FilterCtx` `WithParallel(4)`, ordered | 664 | 200 000 | |
| `FilterCtx` `WithParallel(4)`, `WithUnordered()` | 416 | 28 | |

A parallel stage costs about half a microsecond per element in channel handoffs; ordering adds
roughly 20% on top and two allocations per element (13 MB per 100 000 elements, freed as the
window advances). A CPU profile of the ordered pool on the identity callback puts ~2% of the
time in this library and the rest in the Go runtime: channel mutexes (`lock2`/`unlock2`, 25%),
`select` (28%), `chansend`/`chanrecv` (30%), and waking parked workers (`goready`, `futex`,
`findRunnable`). An element crosses eight channel operations, two of them through a `select`
with `ctx.Done()`, and with a trivial callback the workers drain their input and park, so nearly
every element wakes a goroutine. With `GOMAXPROCS=1` the same pool runs about 120 ns per element
faster (521 vs 637 ns ordered, 357 vs 477 unordered): that difference is cross-core traffic and
OS-thread wake-ups, the price of using several cores at all, which the I/O tables below show
being repaid many times over. Rule of thumb from these numbers: **a callback has to cost more than a few
microseconds before `WithParallel` pays**, which every network or disk call does.

### Break-even: when does a pool start paying?

`BenchmarkPool_BreakEven` (added 2026-09-20, three runs): 2 000 elements, a callback that **burns
CPU** for the given time (not sleeps, so eight workers compete for the eight cores like real
work), `WithParallel(1)` against `WithParallel(8)`.

| Callback cost | sequential | 8 workers | 8 workers vs sequential |
| ---: | ---: | ---: | ---: |
| 0 | 19 µs | 1 283 µs | 66× slower (pure handoff, ~640 ns/element) |
| 1 µs | 2.12 ms | 3.23 ms | 1.5× slower |
| 5 µs | 10.2 ms | 6.0 ms | 1.7× faster |
| 20 µs | 40.2 ms | 9.6 ms | 4.2× faster |
| 100 µs | 201 ms | 25.9 ms | 7.8× faster |

The break-even sits between 1 and 5 µs of work per element, and the pool approaches the core
count from about 100 µs. Below the break-even a pool is not "a bit slower", it is an order of
magnitude slower, because the callback is cheaper than the handoff. That is the whole rule:
a parallel stage is for calls that leave the process, or for CPU work in the tens of
microseconds and up; everything cheaper belongs in `Map`.

### Simulated I/O: what the pool is for

`ioN = 1 000` elements, each sleeping 200 µs; "skewed" makes every eighth element ten times
slower (2 ms), the case where ordering could stall the line. `WithParallel(8)`.

| Pipeline | wall time / run | vs sequential | allocs / run |
| --- | ---: | ---: | ---: |
| uniform, sequential | 1 067 ms | 1.0× | 13 |
| uniform, parallel 8, ordered | 135.0 ms | 7.9× | 2 038 |
| uniform, parallel 8, unordered | 134.5 ms | 7.9× | 36 |
| skewed, sequential | 1 205 ms | 1.0× | 13 |
| skewed, parallel 8, ordered | 151.6 ms | 7.9× | 2 039 |
| skewed, parallel 8, unordered | 150.3 ms | 8.0× | 36 |

With eight workers the speed-up is 7.9× in every case, and ordered is within 1% of unordered,
also with the skew: the read-ahead window (`2n = 16`) absorbs a slow item as long as its
neighbours are not all slow too. Head-of-line blocking is real but needs a pathological item,
one that is slower than the whole window's worth of work; a single 200 ms call in a stream of
200 µs calls would stall the stage for 200 ms in ordered mode and for nothing in unordered mode.
`WithUnordered()` is the right call when that is the workload and order does not matter.

## 4. Fan-in

Four sources of 25 000 elements each.

| Pipeline | ns / element | allocs / run |
| --- | ---: | ---: |
| `Concat` | 7.8 | 23 |
| `Merge` | 150 | 34 |

`Merge` is a channel per source plus one goroutine each; `Concat` is a loop. Use `Merge` for
sources that block, `Concat` for everything else.

## 5. `step` decorators

Identity callback that never fails, so the number is the middleware chain, not the retries.

| Callback | ns / element | allocs / element | Notes |
| --- | ---: | ---: | --- |
| bare `MapCtx` | 9.4 | 0 | |
| `Decorate(fn, Retry(3))` | 58.8 | 3 | closures for the chain and the call |
| `Decorate(fn, Retry(3), Timeout(1s), Tolerate(err))` | 796 | 9 | `context.WithTimeout` per call: a timer and a context, ~700 ns |

`Retry` alone costs about 50 ns per call. `Timeout` is the expensive one, and inherently so:
a deadline is a timer. Both are noise next to the I/O they exist for.

## Reproducing

```bash
make bench                # 8 runs of everything into bench.out, then benchstat
BENCH_COUNT=3 make bench  # quicker, noisier
```

`bench.out` is git-ignored. Run on a quiet machine with a fixed CPU governor for tighter
intervals; the ±20% cases above are GC scheduling on a busy laptop, not the code.

Pull requests get the same suite run against their base by `.github/workflows/bench.yml`, with
the `benchstat` comparison in the job summary. It is advisory: hosted runners are noisy, so a
delta counts when its confidence interval clears zero and it survives a local `make bench`.

## Raw benchstat output

```
Op_Identity-16                         1.026m ±  5%    584 B/op       13 allocs/op
Op_Take-16                             901.2µ ±  6%    440 B/op       11 allocs/op
Op_Skip-16                             1.431m ±  4%    448 B/op       11 allocs/op
Op_Chunk100-16                         746.3µ ± 14%  876.3Ki B/op   1012 allocs/op
Op_Chunk100_Flatten-16                 1.204m ± 13%  876.5Ki B/op   1015 allocs/op
Op_Compact-16                          1.147m ±  3%    824 B/op       19 allocs/op
Op_Transform_Identity-16               1.032m ±  5%    505 B/op       13 allocs/op
Op_ReduceBy10Keys-16                   2.386m ±  6%    913 B/op       13 allocs/op
Op_Zip-16                              8.985m ±  2%    888 B/op       20 allocs/op   (separate run, same machine; Op_Identity that run: 976.1µ ± 2%)
Op_CircuitBreaker_10pctErrors-16       6.484m ± 24%  1.528Mi B/op  50020 allocs/op
Op_Suppress_10pctErrors-16             2.197m ± 14%  469.3Ki B/op  20010 allocs/op
Pool_MapCtx_Sequential-16              948.1µ ±  2%    560 B/op       12 allocs/op
Pool_MapCtx_Parallel4_Ordered-16       57.19m ±  3%  12.97Mi B/op   200.0k allocs/op
Pool_MapCtx_Parallel4_Unordered-16     47.75m ±  8%  1.595Ki B/op     24 allocs/op
Pool_FilterCtx_Parallel4_Ordered-16    66.43m ± 12%  13.73Mi B/op   200.0k allocs/op
Pool_FilterCtx_Parallel4_Unordered-16  41.60m ±  5%  1.606Ki B/op     28 allocs/op
IO_Uniform_Sequential-16                1.067 ±  1%    656 B/op       13 allocs/op
IO_Uniform_Parallel8_Ordered-16        135.0m ±  1%  135.9Ki B/op   2038 allocs/op
IO_Uniform_Parallel8_Unordered-16      134.5m ±  1%  2.586Ki B/op     36 allocs/op
IO_Skewed_Sequential-16                 1.205 ±  1%    656 B/op       13 allocs/op
IO_Skewed_Parallel8_Ordered-16         151.6m ±  0%  136.1Ki B/op   2039 allocs/op
IO_Skewed_Parallel8_Unordered-16       150.3m ±  2%  2.755Ki B/op     36 allocs/op
Combine_Merge4-16                      14.99m ±  1%  2.165Ki B/op     34 allocs/op
Combine_Concat4-16                     776.7µ ±  6%  1.063Ki B/op     23 allocs/op
Step_Bare-16                           942.9µ ±  4%    560 B/op       12 allocs/op
Step_Retry-16                          5.879m ±  8%  7.630Mi B/op   300.0k allocs/op
Step_Retry_Timeout_Tolerate-16         79.59m ± 25%  38.15Mi B/op   900.0k allocs/op
Mem_MapFilterCollect-16                723.1µ ±  9%  1.107Mi B/op     30 allocs/op
Stream_MapFilterCollect-16             1.979m ± 25%  1.108Mi B/op     45 allocs/op
Mem_ChunkFlattenReduce-16              774.1µ ±  8%  876.1Ki B/op   1008 allocs/op
Stream_ChunkFlattenReduce-16           2.149m ± 23%  876.6Ki B/op   1017 allocs/op
Baseline_PlainLoop-16                  336.0µ ± 19%  1.107Mi B/op     23 allocs/op
```
