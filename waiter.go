package batcher

import (
	"context"
	"sync"
)

type waiter[K comparable, V any] struct {
	ctx context.Context
	key K
	res chan *Result[K, V]

	isClosed bool
	mu       sync.Mutex
}

func newWaiter[K comparable, V any](ctx context.Context, key K, responseChannel chan *Result[K, V]) *waiter[K, V] {
	return &waiter[K, V]{
		ctx: ctx,
		key: key,
		res: responseChannel,
	}
}

func (w *waiter[K, V]) Response(result *Result[K, V]) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isClosed {
		return
	}

	w.res <- result
}

func (w *waiter[K, V]) Error(err error) {
	w.Response(&Result[K, V]{Key: w.key, Err: err})
}

func (w *waiter[K, V]) Value() <-chan *Result[K, V] {
	return w.res
}

func (w *waiter[K, V]) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.isClosed = true
}
