package batcher

import (
	"context"
	"fmt"
	"time"
)

type wrapper[K comparable, V any] struct {
	handler      func(context.Context, []K) []*Result[V]
	cache        Cache[K, V]
	cacheGetMany CacheGetMany[K, V]
	timeout      time.Duration
}

func wrapHandler[K comparable, V any](cache Cache[K, V], timeout time.Duration, handler func(context.Context, []K) []*Result[V]) *wrapper[K, V] {
	w := &wrapper[K, V]{handler: handler, timeout: timeout}

	if cache != nil {
		w.cache = cache

		if cmany, ok := cache.(CacheGetMany[K, V]); ok {
			w.cacheGetMany = cmany
		}
	}

	return w
}

func (w *wrapper[K, V]) Handle(ctx context.Context, keys []K) (result []*Result[V]) {
	// Detache ctx if needed
	if w.timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(DetachedContext(ctx), w.timeout)
		defer cancel()
	}

	// TODO use cache

	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("%w: %#v", ErrPanicRecover, r)

			result = make([]*Result[V], 0, len(keys))
			for range keys {
				result = append(result, &Result[V]{Err: err})
			}
		}
	}()

	return w.handler(ctx, keys)
}
