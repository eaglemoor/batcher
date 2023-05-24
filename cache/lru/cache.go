package lru

import (
	"context"

	hlru "github.com/hashicorp/golang-lru/v2"
)

type LRUCache[K comparable, V any] struct {
	cache *hlru.ARCCache[K, V]
}

func New[K comparable, V any](size int) (*LRUCache[K, V], error) {
	arc, err := hlru.NewARC[K, V](size)
	if err != nil {
		return nil, err
	}

	return &LRUCache[K, V]{cache: arc}, nil
}

func (c *LRUCache[K, V]) Get(ctx context.Context, key K) (V, bool) {
	if c.cache == nil {
		var v V

		return v, false
	}

	return c.cache.Get(key)
}

func (c *LRUCache[K, V]) Put(ctx context.Context, key K, value V) {
	if c.cache == nil {
		return
	}

	c.cache.Add(key, value)
}
