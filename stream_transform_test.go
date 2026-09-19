package chunkflow_test

import (
	"context"
	"iter"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStream_Transform(t *testing.T) {
	ctx := t.Context()

	t.Run("sees every element, fatal errors included, and may keep going", func(t *testing.T) {
		var got []string
		observe := func(in iter.Seq2[int, error]) iter.Seq2[int, error] {
			return func(yield func(int, error) bool) {
				for v, err := range in {
					if err != nil {
						got = append(got, "err")
						continue // swallow: a policy may drop what Seq would have stopped at
					}
					got = append(got, strconv.Itoa(v))
					if !yield(v, nil) {
						return
					}
				}
			}
		}
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 6)).MapCtx(failing).Transform(observe).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 5}, res)
		assert.Equal(t, []string{"0", "1", "err", "err", "err", "5"}, got, "Seq() would have stopped at the first error")
	})

	t.Run("may change the element type", func(t *testing.T) {
		toString := func(in iter.Seq2[int, error]) iter.Seq2[string, error] {
			return func(yield func(string, error) bool) {
				for v, err := range in {
					if !yield(strconv.Itoa(v), err) {
						return
					}
				}
			}
		}
		res, err := chunkflow.New(ctx).Seq(seq.Range(0, 3)).Transform(toString).Collect()
		require.NoError(t, err)
		assert.Equal(t, []string{"0", "1", "2"}, res)
	})

	t.Run("keeps the context and the pipeline options", func(t *testing.T) {
		type ctxKey struct{}
		cctx := context.WithValue(ctx, ctxKey{}, "marker")
		var reported []error
		identity := func(in iter.Seq2[int, error]) iter.Seq2[int, error] { return in }

		var seenCtx atomic.Bool // WithParallel(3) below is inherited, so workers race on a plain bool
		res, err := chunkflow.New(cctx).Seq(seq.Range(0, 4)).
			Opts(chunkflow.WithOnError(func(err error) { reported = append(reported, err) }), chunkflow.WithParallel(3)).
			Transform(identity).
			MapCtx(func(ctx context.Context, i int) (int, error) {
				if ctx.Value(ctxKey{}) == "marker" {
					seenCtx.Store(true)
				}
				if i == 2 {
					return 0, chunkflow.Suppress(errBoom)
				}
				return i, nil
			}).
			Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 3}, res)
		assert.True(t, seenCtx.Load(), "context values must survive Transform")
		require.Len(t, reported, 1, "WithOnError must survive Transform")
		assert.ErrorIs(t, reported[0], errBoom)
	})

	t.Run("short-circuits upstream when the consumer stops", func(t *testing.T) {
		pulled := 0
		identity := func(in iter.Seq2[int, error]) iter.Seq2[int, error] { return in }
		res, err := chunkflow.New(ctx).Seq(seq.Numbers(0)).Tap(func(int) { pulled++ }).Transform(identity).Take(3).Collect()
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 2}, res)
		assert.Equal(t, 3, pulled)
	})

	t.Run("nil transform is an error stream", func(t *testing.T) {
		_, err := chunkflow.New(ctx).Seq(seq.Items(1)).Transform[int](nil).Collect()
		require.ErrorContains(t, err, "Transform: nil transform")
	})

	t.Run("runs once at build time; per-iteration state belongs inside the iterator", func(t *testing.T) {
		builds := 0
		counting := func(in iter.Seq2[int, error]) iter.Seq2[int, error] {
			builds++
			return func(yield func(int, error) bool) {
				n := 0 // fresh on every iteration
				for v, err := range in {
					n++
					if !yield(v*n, err) {
						return
					}
				}
			}
		}
		s := chunkflow.New(ctx).Seq(seq.Items(1, 1, 1)).Transform(counting)
		for range 2 {
			res, err := s.Collect()
			require.NoError(t, err)
			assert.Equal(t, []int{1, 2, 3}, res)
		}
		assert.Equal(t, 1, builds)
	})

	t.Run("a panic in extension code is not a callback panic", func(t *testing.T) {
		exploding := func(iter.Seq2[int, error]) iter.Seq2[int, error] {
			return func(func(int, error) bool) { panic("extension bug") }
		}
		assert.PanicsWithValue(t, "extension bug", func() {
			_, _ = chunkflow.New(ctx).Seq(seq.Items(1)).Transform(exploding).Collect()
		})
	})
}
