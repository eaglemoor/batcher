package batcher

import "context"

type Cache[K comparable, V any] interface {
	Get(context.Context, K) (V, bool)
	Put(context.Context, K, V)
}

type CacheGetMany[K comparable, V any] interface {
	GetMany(context.Context, []K) map[K]V
}
