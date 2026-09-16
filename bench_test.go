package chunkflow_test

import (
	"context"
	"iter"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
)

// memStream is the in-memory, infallible pipeline that chunkflow shipped as Stream[T]
// until v0.1.0. It was removed from the library because Stream (the former Stream) is a
// strict superset, and keeping two types meant every operator, test and doc twice. It
// lives on here, private, as the reference point for what the error/context plumbing
// costs on pure data. If the numbers ever matter to someone, this is the starting point
// for a separate type.
type memStream[T any] struct{ seq iter.Seq[T] }

func newMem[T any](s iter.Seq[T]) memStream[T] { return memStream[T]{seq: s} }

func (a memStream[T]) Map[R any](fn func(T) R) memStream[R] {
	return newMem(func(yield func(R) bool) {
		for v := range a.seq {
			if !yield(fn(v)) {
				return
			}
		}
	})
}

func (a memStream[T]) Filter(pred func(T) bool) memStream[T] {
	return newMem(func(yield func(T) bool) {
		for v := range a.seq {
			if pred(v) && !yield(v) {
				return
			}
		}
	})
}

// Chunk takes R as an explicit type parameter for the same reason Stream.Chunk does:
// returning memStream[[]T] directly forms a generic instantiation cycle (T, []T, [][]T, ...).
func (a memStream[T]) Chunk[R []T](size int) memStream[R] {
	return newMem(func(yield func(R) bool) {
		chunk := make([]T, 0, size)
		for v := range a.seq {
			chunk = append(chunk, v)
			if len(chunk) == size {
				if !yield(chunk) {
					return
				}
				chunk = make([]T, 0, size)
			}
		}
		if len(chunk) > 0 {
			yield(chunk)
		}
	})
}

func memFlatten[E any](a memStream[[]E]) memStream[E] {
	return newMem(func(yield func(E) bool) {
		for chunk := range a.seq {
			for _, v := range chunk {
				if !yield(v) {
					return
				}
			}
		}
	})
}

func (a memStream[T]) Reduce[R any](init R, fn func(R, T) R) R {
	acc := init
	for v := range a.seq {
		acc = fn(acc, v)
	}
	return acc
}

func (a memStream[T]) Collect() []T {
	var out []T
	for v := range a.seq {
		out = append(out, v)
	}
	return out
}

const benchN = 100_000

var (
	benchSink    []int
	benchSinkInt int
)

func BenchmarkMem_MapFilterCollect(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchSink = newMem(seq.Range(0, benchN)).
			Map(func(i int) int { return i * 2 }).
			Filter(func(i int) bool { return i%3 == 0 }).
			Collect()
	}
}

func BenchmarkStream_MapFilterCollect(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		benchSink, _ = chunkflow.New(ctx).Seq(seq.Range(0, benchN)).
			Map(func(i int) int { return i * 2 }).
			Filter(func(i int) bool { return i%3 == 0 }).
			Collect()
	}
}

func BenchmarkMem_ChunkFlattenReduce(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchSinkInt = memFlatten(newMem(seq.Range(0, benchN)).Chunk[[]int](100)).
			Reduce(0, func(acc, i int) int { return acc + i })
	}
}

func BenchmarkStream_ChunkFlattenReduce(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		benchSinkInt, _ = chunkflow.New(ctx).Seq(seq.Range(0, benchN)).
			Chunk[[]int](100).
			Through(chunkflow.Flatten).
			Reduce(0, func(acc, i int) int { return acc + i })
	}
}

// Baseline: the same work as a plain loop, to see what any abstraction costs at all.
func BenchmarkBaseline_PlainLoop(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		var out []int
		for i := range benchN {
			v := i * 2
			if v%3 == 0 {
				out = append(out, v)
			}
		}
		benchSink = out
	}
}
