package chunkflow_test

import (
	"slices"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/policy"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStream_Compact(t *testing.T) {
	ctx := t.Context()

	t.Run("drops only consecutive duplicates", func(t *testing.T) {
		res, err := chunkflow.New(ctx).Seq(seq.Items(1, 1, 2, 2, 2, 3, 1, 1)).
			Through(chunkflow.Compact).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3, 1}, res)
	})

	t.Run("CompactFunc with custom equality keeps the first of each run", func(t *testing.T) {
		type row struct{ key, payload string }
		res, err := chunkflow.
			New(ctx).Seq(slices.Values([]row{
			{"a", "1"},
			{"a", "2"},
			{"b", "3"},
			{"a", "5"},
		})).
			CompactFunc(func(x, y row) bool { return x.key == y.key }).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []row{{"a", "1"}, {"b", "3"}, {"a", "5"}}, res)
	})

	t.Run("errors pass through and do not reset the comparison", func(t *testing.T) {
		// values: 1, err, 1, 2 -> the second 1 is still a consecutive duplicate
		src := func(yield func(int, error) bool) {
			for _, step := range []struct {
				v   int
				err error
			}{{1, nil}, {0, errBoom}, {1, nil}, {2, nil}} {
				if !yield(step.v, step.err) {
					return
				}
			}
		}
		var values []int
		var suppressed int
		for v, err := range chunkflow.New(ctx).SeqErr(src).Through(chunkflow.Compact).Through(policy.CircuitBreaker[int](100)).Seq() {
			if err != nil {
				require.ErrorIs(t, err, chunkflow.ErrSuppressed)
				suppressed++
				continue
			}
			values = append(values, v)
		}
		assert.Equal(t, []int{1, 2}, values)
		assert.Equal(t, 1, suppressed, "the error itself is not swallowed")
	})

	t.Run("is fail-fast without a breaker", func(t *testing.T) {
		res, err := chunkflow.New(ctx).SeqErr(errAfter(3, errBoom)).Through(chunkflow.Compact).Collect()
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, []int{0, 1, 2}, res)
	})

	t.Run("stops pulling once the consumer is done", func(t *testing.T) {
		pulled := 0
		v, err := chunkflow.New(ctx).Seq(seq.Const(7)).
			Tap(func(int) { pulled++ }).
			Through(chunkflow.Compact).
			First()
		require.NoError(t, err)
		assert.Equal(t, 7, v)
		assert.Equal(t, 1, pulled)
	})

	t.Run("CompactFunc receives values in (previous, current) order", func(t *testing.T) {
		var pairs [][2]int
		_, err := chunkflow.New(ctx).Seq(seq.Items(1, 2, 3)).
			CompactFunc(func(prev, cur int) bool {
				pairs = append(pairs, [2]int{prev, cur})
				return false
			}).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, [][2]int{{1, 2}, {2, 3}}, pairs)
	})
}
