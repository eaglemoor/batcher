package batcher

import "time"

type opt[K comparable, V any] struct {
	MaxBatchSize int
	MinBatchSize int
	Timeout      time.Duration
	MaxHandlers  int
	Ticker       time.Duration

	Cache Cache[K, V]
}

type Option[K comparable, V any] func(*opt[K, V])

func MaxBatchSize[K comparable, V any](n int) Option[K, V] {
	return func(o *opt[K, V]) {
		if n > 0 {
			o.MaxBatchSize = n
		}
	}
}

func MinBatchSize[K comparable, V any](n int) Option[K, V] {
	return func(o *opt[K, V]) {
		if n > 0 {
			o.MinBatchSize = n
		}
	}
}

func MaxHandlers[K comparable, V any](n int) Option[K, V] {
	return func(o *opt[K, V]) {
		if n > 0 {
			o.MaxHandlers = n
		}
	}
}

func Timeout[K comparable, V any](d time.Duration) Option[K, V] {
	return func(o *opt[K, V]) {
		o.Timeout = d
	}
}

func Ticker[K comparable, V any](d time.Duration) Option[K, V] {
	return func(o *opt[K, V]) {
		if d > 0 {
			o.Ticker = d
		}
	}
}

func WithCache[K comparable, V any](cache Cache[K, V]) Option[K, V] {
	return func(o *opt[K, V]) {
		if cache != nil {
			o.Cache = cache
		}
	}
}
