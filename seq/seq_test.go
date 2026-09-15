package seq_test

import (
	"slices"
	"testing"

	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
)

func take[T any](n int, s func(func(T) bool)) []T {
	var out []T
	for v := range s {
		if len(out) == n {
			break
		}
		out = append(out, v)
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
