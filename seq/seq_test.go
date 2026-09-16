package seq_test

import (
	"slices"
	"testing"

	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
)

func take[T any](n int, s func(func(T) bool)) []T {
	var out []T
	if n <= 0 {
		return out
	}
	for v := range s {
		out = append(out, v)
		if len(out) == n {
			break // stop right away; do not pull one element past the limit
		}
	}
	return out
}

func TestItems(t *testing.T) {
	assert.Equal(t, []string{"a", "b"}, slices.Collect(seq.Items("a", "b")))
	assert.Empty(t, slices.Collect(seq.Items[int]()))
}

func TestRange(t *testing.T) {
	assert.Equal(t, []int{1, 2, 3}, slices.Collect(seq.Range(1, 4)))
	assert.Empty(t, slices.Collect(seq.Range(4, 1)), "from >= to yields nothing")
	assert.Equal(t, []uint8{250, 251}, slices.Collect(seq.Range[uint8](250, 252)))
	assert.Equal(t, []int{1, 2}, take(2, seq.Range(1, 10)), "must honour early termination")
}

func TestRangeInclusive(t *testing.T) {
	assert.Equal(t, []int{1, 2, 3, 4}, slices.Collect(seq.RangeInclusive(1, 4)))
	assert.Equal(t, []int{7}, slices.Collect(seq.RangeInclusive(7, 7)))
	assert.Empty(t, slices.Collect(seq.RangeInclusive(4, 1)), "from > to yields nothing")
	assert.Equal(t, []uint8{254, 255}, slices.Collect(seq.RangeInclusive[uint8](254, 255)), "must not overflow at the upper bound")
	assert.Equal(t, []int{1, 2}, take(2, seq.RangeInclusive(1, 10)), "must honour early termination")
}

func TestNumbers(t *testing.T) {
	assert.Equal(t, []int{5, 6, 7}, take(3, seq.Numbers(5)))
}

func TestConst(t *testing.T) {
	assert.Equal(t, []string{"x", "x"}, take(2, seq.Const("x")))
}

func TestRepeat(t *testing.T) {
	assert.Equal(t, []string{"x", "x", "x"}, slices.Collect(seq.Repeat("x", 3)))
	assert.Empty(t, slices.Collect(seq.Repeat("x", 0)))
	assert.Empty(t, slices.Collect(seq.Repeat("x", -5)), "negative n yields nothing")
	assert.Equal(t, []string{"x", "x"}, take(2, seq.Repeat("x", 10)), "must honour early termination")
}

func TestIterate(t *testing.T) {
	double := func(x int) int { return x * 2 }
	assert.Equal(t, []int{1, 2, 4, 8, 16}, take(5, seq.Iterate(1, double)))

	t.Run("seed is yielded before fn is ever called", func(t *testing.T) {
		calls := 0
		s := seq.Iterate(7, func(x int) int { calls++; return x + 1 })
		assert.Equal(t, []int{7}, take(1, s))
		assert.Equal(t, 0, calls)
	})

	t.Run("every pass restarts from the seed", func(t *testing.T) {
		s := seq.Iterate(1, double)
		assert.Equal(t, []int{1, 2, 4}, take(3, s))
		assert.Equal(t, []int{1, 2, 4}, take(3, s))
	})

	t.Run("works with non-numeric state", func(t *testing.T) {
		fib := seq.Iterate([2]int{0, 1}, func(p [2]int) [2]int { return [2]int{p[1], p[0] + p[1]} })
		var firsts []int
		for p := range fib {
			if len(firsts) == 8 {
				break
			}
			firsts = append(firsts, p[0])
		}
		assert.Equal(t, []int{0, 1, 1, 2, 3, 5, 8, 13}, firsts)
	})
}
