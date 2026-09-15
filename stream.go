package chunkflow

import "iter"

// Stream represents a lazily evaluated pipeline operating on elements of type T.
// Transforming operations do not allocate memory until a terminal operation is called.
type Stream[T any] struct {
	seq iter.Seq[T]
}

// NewStream initializes a stream based on a native Go iterator.
func NewStream[T any](seq iter.Seq[T]) Stream[T] {
	return Stream[T]{seq: seq}
}

// Map transforms each element of the stream using the provided function.
func (a Stream[T]) Map[R any](mapFn func(T) R) Stream[R] {
	return NewStream(func(yield func(R) bool) {
		for val := range a.seq {
			if !yield(mapFn(val)) {
				return
			}
		}
	})
}

// Take consumes at most nr elements from the stream and then short-circuits.
func (a Stream[T]) Take(nr int) Stream[T] {
	return NewStream(func(yield func(T) bool) {
		i := 0
		for val := range a.seq {
			if i == nr {
				return
			}
			i++
			if !yield(val) {
				return
			}
		}
	})
}

// Skip bypasses the first nr elements and emits the remainder of the stream.
func (a Stream[T]) Skip(nr int) Stream[T] {
	return NewStream(func(yield func(T) bool) {
		skipped := 0
		for val := range a.seq {
			if skipped < nr {
				skipped++
				continue
			}
			if !yield(val) {
				return
			}
		}
	})
}

// Filter emits only the elements for which the predicate returns true.
func (a Stream[T]) Filter(predicate func(T) bool) Stream[T] {
	return NewStream(func(yield func(T) bool) {
		for val := range a.seq {
			if !predicate(val) {
				continue
			}
			if !yield(val) {
				return
			}
		}
	})
}

// Chunk groups elements into slices of the given size.
// The final chunk may contain fewer elements than size. Panics if size < 1.
func (a Stream[T]) Chunk[R []T](size int) Stream[R] {
	if size < 1 {
		panic("invalid chunk size")
	}

	return NewStream(func(yield func(R) bool) {
		chunk := make([]T, 0, size)
		for val := range a.seq {
			chunk = append(chunk, val)
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

// Through pipes the current stream into an external transformation function.
// It acts as a structural bridge to maintain fluent API chaining for operations that
// must be implemented as top-level functions (like Flatten) due to Go's type system constraints.
func (a Stream[T]) Through[R any](transform func(Stream[T]) Stream[R]) Stream[R] {
	return transform(a)
}

// Reduce is a terminal operation that aggregates the stream into a single value.
func (a Stream[T]) Reduce(init T, fn func(item, acc T) T) T {
	for item := range a.seq {
		init = fn(item, init)
	}
	return init
}

// Seq returns the underlying native Go iterator for integration with standard packages.
func (a Stream[T]) Seq() iter.Seq[T] {
	return a.seq
}

// All is a terminal operation that verifies if all elements satisfy the predicate.
// Returns false and short-circuits on the first mismatch.
func (a Stream[T]) All(predicate func(T) bool) bool {
	for val := range a.seq {
		if !predicate(val) {
			return false
		}
	}
	return true
}

// Any is a terminal operation that verifies if at least one element satisfies the predicate.
// Returns true and short-circuits on the first match.
func (a Stream[T]) Any(predicate func(T) bool) bool {
	for val := range a.seq {
		if predicate(val) {
			return true
		}
	}
	return false
}

// Collect is a terminal operation that materializes the stream into a slice allocated on the heap.
// Unsafe for infinite streams.
func (a Stream[T]) Collect() []T {
	var result []T
	for val := range a.seq {
		result = append(result, val)
	}
	return result
}

// ForEach is a terminal operation that executes a side effect for each element.
func (a Stream[T]) ForEach(fn func(T)) {
	for val := range a.seq {
		fn(val)
	}
}

// Exec is a terminal operation that exhausts the stream to force the execution of side effects.
func (a Stream[T]) Exec() {
	for range a.seq {
	}
}

// First consumes at most one element. Returns the element and true, or a zero value and false if the stream was empty.
func (a Stream[T]) First() (T, bool) {
	for item := range a.seq {
		return item, true
	}
	var zero T
	return zero, false
}

// Last consumes the entire stream and returns the final visited element.
func (a Stream[T]) Last() (T, bool) {
	var item T
	var ok bool
	for item = range a.seq {
		ok = true
	}
	return item, ok
}

// Flatten unwraps a stream of slices into a continuous, flat stream of individual elements.
// It is implemented as a top-level function rather than a method because Go's compiler
// does not support narrowing receiver type constraints (i.e., enforcing that T must be a slice).
func Flatten[E any](stream Stream[[]E]) Stream[E] {
	return NewStream(func(yield func(E) bool) {
		for chunk := range stream.seq {
			for _, val := range chunk {
				if !yield(val) {
					return
				}
			}
		}
	})
}
