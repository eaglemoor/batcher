package batcher

import "time"

type opt[K comparable, V any] struct {
	MaxBatchSize int
	MinBatchSize int
	Timeout      time.Duration
	MaxBatcher   int
	Ticker       time.Duration

	Cache Cache[K, V]
}

type Option[K comparable, V any] func(*opt[K, V])

// MaxBatchSize setup maximum number of keys that fit in one call of batchFunc
func MaxBatchSize[K comparable, V any](n int) Option[K, V] {
	return func(o *opt[K, V]) {
		if n > 0 {
			o.MaxBatchSize = n
		}
	}
}

// MinBatchSize setup minimal number of keys that fit in one call
// If the service is not loaded, then a smaller number of keys can be used
func MinBatchSize[K comparable, V any](n int) Option[K, V] {
	return func(o *opt[K, V]) {
		if n > 0 {
			o.MinBatchSize = n
		}
	}
}

// MaxBatcher setup maximum number of threads for processing requests
func MaxBatcher[K comparable, V any](n int) Option[K, V] {
	return func(o *opt[K, V]) {
		if n > 0 {
			o.MaxBatcher = n
		}
	}
}

// Timeout maximum execution time for user requests
func Timeout[K comparable, V any](d time.Duration) Option[K, V] {
	return func(o *opt[K, V]) {
		o.Timeout = d
	}
}

// Ticker time batcher launch interval. necessary for processing the queue
// when the minimum batch size is >1 and at low load, records can get stuck in it without processing
//
// Default: 17ms
func Ticker[K comparable, V any](d time.Duration) Option[K, V] {
	return func(o *opt[K, V]) {
		if d > 0 {
			o.Ticker = d
		}
	}
}

// WithCache using cache for load / store data
func WithCache[K comparable, V any](cache Cache[K, V]) Option[K, V] {
	return func(o *opt[K, V]) {
		if cache != nil {
			o.Cache = cache
		}
	}
}
