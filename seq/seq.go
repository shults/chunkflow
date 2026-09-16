// Package seq provides generators of native Go iterators (iter.Seq) that can be
// fed into chunkflow.NewStream or chunkflow.NewIo(ctx).Seq, or used directly in
// range-over-func loops.
package seq

import (
	"iter"
	"slices"
)

// Integer is the set of built-in integer types accepted by Range and Numbers.
type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// Items yields the provided arguments in order. The sequence is finite.
func Items[T any](items ...T) iter.Seq[T] {
	return slices.Values(items)
}

// Range yields integers in the half-open interval [from, to).
// If from >= to the sequence is empty. The sequence is finite.
func Range[T Integer](from, to T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for i := from; i < to; i++ {
			if !yield(i) {
				return
			}
		}
	}
}

// RangeInclusive yields integers in the closed interval [from, to].
// If from > to the sequence is empty. The sequence is finite.
// It is implemented without computing to+1 so that to may be the maximum value of T.
func RangeInclusive[T Integer](from, to T) iter.Seq[T] {
	return func(yield func(T) bool) {
		if from > to {
			return
		}
		for i := from; ; i++ {
			if !yield(i) || i == to {
				return
			}
		}
	}
}

// Numbers yields a monotonically increasing sequence starting at start.
// The sequence is infinite; combine it with Take or another short-circuiting operation.
func Numbers[T Integer](start T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for i := start; ; i++ {
			if !yield(i) {
				return
			}
		}
	}
}

// Repeat yields val exactly n times. It is the finite form of Const; n <= 0 yields nothing.
func Repeat[T any](val T, n int) iter.Seq[T] {
	return func(yield func(T) bool) {
		for range max(n, 0) {
			if !yield(val) {
				return
			}
		}
	}
}

// Iterate yields seed, fn(seed), fn(fn(seed)), ... forever. The sequence is infinite;
// combine it with Take, TakeWhile or another short-circuiting operation. Every
// iteration starts again from seed, so the sequence can be ranged over repeatedly.
// Numbers(start) is the special case Iterate(start, func(x) x+1).
func Iterate[T any](seed T, fn func(T) T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for cur := seed; ; cur = fn(cur) {
			if !yield(cur) {
				return
			}
		}
	}
}

// Const yields the same value forever. The sequence is infinite.
func Const[T any](val T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			if !yield(val) {
				return
			}
		}
	}
}
