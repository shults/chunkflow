package chunkflow_test

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/policy"
	"github.com/shults/chunkflow/seq"
	"github.com/shults/chunkflow/step"
)

// Per-operator benchmarks. Every pipeline processes benchN ints and ends in Drain unless the
// operator's own output is the point, so that the number is the operator's cost, not the
// terminal's. BenchmarkOp_Identity is the floor: source, one trivial Map and Drain.

var errBench = errors.New("bench")

func benchSource(ctx context.Context) chunkflow.Stream[int] {
	return chunkflow.New(ctx).Seq(seq.Range(0, benchN))
}

func BenchmarkOp_Identity(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).Map(func(i int) int { return i }).Drain()
	}
}

func BenchmarkOp_Take(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = chunkflow.New(ctx).Seq(seq.Numbers(0)).Take(benchN).Drain()
	}
}

func BenchmarkOp_Skip(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = chunkflow.New(ctx).Seq(seq.Range(0, 2*benchN)).Skip(benchN).Drain()
	}
}

func BenchmarkOp_Chunk100(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).Chunk[[]int](100).Drain()
	}
}

func BenchmarkOp_Chunk100_Flatten(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).Chunk[[]int](100).Through(chunkflow.Flatten).Drain()
	}
}

func BenchmarkOp_Compact(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		// runs of ten equal values: Compact keeps one in ten
		benchSinkInt, _ = benchSource(ctx).Map(func(i int) int { return i / 10 }).Through(chunkflow.Compact).Count()
	}
}

func BenchmarkOp_Transform_Identity(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).Transform(func(in iter.Seq2[int, error]) iter.Seq2[int, error] { return in }).Drain()
	}
}

func BenchmarkOp_ReduceBy10Keys(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = benchSource(ctx).ReduceBy(func(i int) int { return i % 10 }, 0, func(n, _ int) int { return n + 1 })
	}
}

func BenchmarkOp_Zip(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).Zip(benchSource(ctx), func(a, c int) int { return a + c }).Drain()
	}
}

// Error-path operators: every tenth element fails, nothing trips.

func failEveryTenth(_ context.Context, i int) (int, error) {
	if i%10 == 0 {
		return 0, errBench
	}
	return i, nil
}

func BenchmarkOp_CircuitBreaker_10pctErrors(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).MapCtx(failEveryTenth).Through(policy.CircuitBreaker[int](1_000_000)).Drain()
	}
}

func BenchmarkOp_Suppress_10pctErrors(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).MapCtx(func(ctx context.Context, i int) (int, error) {
			v, err := failEveryTenth(ctx, i)
			return v, chunkflow.Suppress(err)
		}).Drain()
	}
}

// Worker pools on CPU-trivial work: this measures the pool's own overhead, so the
// sequential variant is expected to win. Ordered (default) vs WithUnordered is the
// price of the reorder window.

func identityCtx(_ context.Context, i int) (int, error) { return i, nil }

func BenchmarkPool_MapCtx_Sequential(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).MapCtx(identityCtx).Drain()
	}
}

func BenchmarkPool_MapCtx_Parallel4_Ordered(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).MapCtx(identityCtx, chunkflow.WithParallel(4)).Drain()
	}
}

func BenchmarkPool_MapCtx_Parallel4_Unordered(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).MapCtx(identityCtx, chunkflow.WithParallel(4), chunkflow.WithUnordered()).Drain()
	}
}

func BenchmarkPool_FilterCtx_Parallel4_Ordered(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).FilterCtx(func(_ context.Context, i int) (bool, error) { return i%2 == 0, nil }, chunkflow.WithParallel(4)).Drain()
	}
}

func BenchmarkPool_FilterCtx_Parallel4_Unordered(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).FilterCtx(func(_ context.Context, i int) (bool, error) { return i%2 == 0, nil }, chunkflow.WithParallel(4), chunkflow.WithUnordered()).Drain()
	}
}

// Worker pools on simulated I/O: ioN elements, each costing ioLatency of wall time. This is
// what the pool is for. "Skewed" makes every eighth element ten times slower, which is where
// ordering pays for itself in head-of-line blocking.

const (
	ioN       = 1_000
	ioLatency = 200 * time.Microsecond
)

func ioUniform(_ context.Context, i int) (int, error) {
	time.Sleep(ioLatency)
	return i, nil
}

func ioSkewed(_ context.Context, i int) (int, error) {
	if i%8 == 0 {
		time.Sleep(10 * ioLatency)
	} else {
		time.Sleep(ioLatency)
	}
	return i, nil
}

func benchIO(b *testing.B, fn func(context.Context, int) (int, error), opts ...chunkflow.StepOption) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = chunkflow.New(ctx).Seq(seq.Range(0, ioN)).MapCtx(fn, opts...).Drain()
	}
}

func BenchmarkIO_Uniform_Sequential(b *testing.B) { benchIO(b, ioUniform) }
func BenchmarkIO_Uniform_Parallel8_Ordered(b *testing.B) {
	benchIO(b, ioUniform, chunkflow.WithParallel(8))
}
func BenchmarkIO_Uniform_Parallel8_Unordered(b *testing.B) {
	benchIO(b, ioUniform, chunkflow.WithParallel(8), chunkflow.WithUnordered())
}
func BenchmarkIO_Skewed_Sequential(b *testing.B) { benchIO(b, ioSkewed) }
func BenchmarkIO_Skewed_Parallel8_Ordered(b *testing.B) {
	benchIO(b, ioSkewed, chunkflow.WithParallel(8))
}
func BenchmarkIO_Skewed_Parallel8_Unordered(b *testing.B) {
	benchIO(b, ioSkewed, chunkflow.WithParallel(8), chunkflow.WithUnordered())
}

// Fan-in: four sources of benchN/4 each, concurrently (Merge) and one after another (Concat).

func BenchmarkCombine_Merge4(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		parts := make([]chunkflow.Stream[int], 4)
		for k := range parts {
			parts[k] = chunkflow.New(ctx).Seq(seq.Range(k*benchN/4, (k+1)*benchN/4))
		}
		_ = chunkflow.Merge(parts...).Drain()
	}
}

func BenchmarkCombine_Concat4(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		parts := make([]chunkflow.Stream[int], 4)
		for k := range parts {
			parts[k] = chunkflow.New(ctx).Seq(seq.Range(k*benchN/4, (k+1)*benchN/4))
		}
		_ = chunkflow.Concat(parts...).Drain()
	}
}

// step decorators on a callback that never fails: the per-call cost of the middleware chain.

func BenchmarkStep_Bare(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).MapCtx(identityCtx).Drain()
	}
}

func BenchmarkStep_Retry(b *testing.B) {
	ctx := context.Background()
	fn := step.Decorate(identityCtx, step.Retry(3))
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).MapCtx(fn).Drain()
	}
}

func BenchmarkStep_Retry_Timeout_Tolerate(b *testing.B) {
	ctx := context.Background()
	fn := step.Decorate(identityCtx, step.Retry(3), step.Timeout(time.Second), step.Tolerate(errBench))
	b.ReportAllocs()
	for b.Loop() {
		_ = benchSource(ctx).MapCtx(fn).Drain()
	}
}
