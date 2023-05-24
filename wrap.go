package batcher

import (
	"context"
	"fmt"
)

type wrapper[K comparable, V any] struct {
	handler      func(context.Context, []K) []*Result[K, V]
	cache        Cache[K, V]
	cacheGetMany CacheGetMany[K, V]
}

func wrapHandler[K comparable, V any](cache Cache[K, V], handler func(context.Context, []K) []*Result[K, V]) *wrapper[K, V] {
	w := &wrapper[K, V]{handler: handler}

	if cache != nil {
		w.cache = cache

		if cmany, ok := cache.(CacheGetMany[K, V]); ok {
			w.cacheGetMany = cmany
		}
	}

	return w
}

func (w *wrapper[K, V]) Handle(ctx context.Context, keys []K) (result []*Result[K, V]) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("%w: %#v", ErrPanicRecover, r)

			result = make([]*Result[K, V], 0, len(keys))
			for _, key := range keys {
				result = append(result, &Result[K, V]{Key: key, Err: err})
			}
		}
	}()

	if w.cache == nil {
		return w.handler(ctx, keys)
	}

	newkeys := scoringKey(keys)

	resultMap := make(map[K]*Result[K, V], len(keys))
	uniqK := make(map[K]struct{}, len(keys))
	reqK := make([]K, 0, len(keys))
	var ok bool

	if w.cacheGetMany != nil {
		items := w.cacheGetMany.GetMany(ctx, newkeys)

		for k, v := range items {
			resultMap[k] = &Result[K, V]{Key: k, Value: v}
			uniqK[k] = struct{}{}
		}

		for _, k := range newkeys {
			_, ok = uniqK[k]
			if ok {
				continue
			}

			reqK = append(reqK, k)
		}
	} else {
		for _, k := range newkeys {
			_, ok = uniqK[k]
			if ok {
				continue
			}

			val, valok := w.cache.Get(ctx, k)
			if valok {
				resultMap[k] = &Result[K, V]{Key: k, Value: val}
			} else {
				reqK = append(reqK, k)
			}

			uniqK[k] = struct{}{}
		}
	}

	if len(reqK) > 0 {
		items := w.handler(ctx, reqK)
		for i, item := range items {
			k := reqK[i]
			resultMap[k] = item

			if item.Err == nil {
				w.cache.Put(ctx, k, item.Value)
			}
		}
	}

	result = make([]*Result[K, V], 0, len(keys))
	for _, key := range keys {
		if val, ok := resultMap[key]; ok {
			result = append(result, val)
		}
	}

	return result
}
