package chunkflow

import "context"

type IoStream[T any] struct {
	ctx context.Context
}

func NewIoStream[T any](ctx context.Context) IoStream[T] {
	return IoStream[T]{}
}
