package batcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultMaxBatchSize = 100
	DefaultMinBatchSize = 1
	DefaultMaxHandlers  = 1
	DefaultTicker       = 17 * time.Millisecond
)

type Result[V any] struct {
	Value V
	Err   error
}

func New[K comparable, V any](handler func(ctx context.Context, keys []K) []*Result[V], opts ...Option[K, V]) *batcher[K, V] {
	o := &opt[K, V]{
		MaxBatchSize: DefaultMaxBatchSize,
		MinBatchSize: DefaultMinBatchSize,
		MaxHandlers:  DefaultMaxHandlers,
		Ticker:       DefaultTicker,
	}

	for _, oo := range opts {
		oo(o)
	}

	ctx, cancel := context.WithCancel(context.Background())

	hwrap := wrapHandler(o.Cache, o.Timeout, handler)

	b := &batcher[K, V]{
		ctx:     ctx,
		opt:     o,
		handler: hwrap.Handle,
		pending: make([]waiter[K, V], 0, o.MaxBatchSize),
		cancel:  cancel,
	}
	go b.timer()

	return b
}

type batcher[K comparable, V any] struct {
	ctx context.Context

	opt     *opt[K, V]
	handler func(context.Context, []K) []*Result[V]

	pending []waiter[K, V]
	mu      sync.Mutex

	nHandlers int
	wg        sync.WaitGroup
	ticker    time.Time

	cancel   func()
	shutdown bool
}

// ticker for catch items by timeout
func (b *batcher[K, V]) timer() {
	ticker := time.NewTicker(b.opt.Ticker)

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			func() {
				b.mu.Lock()
				defer b.mu.Unlock()

				if b.nHandlers < b.opt.MaxHandlers {
					batch := b.nextBatch()
					// log.Printf("batch 17ms %#v\n", batch)
					if batch != nil {
						b.wg.Add(1)
						go func() {
							b.callHandlers(batch)
							b.wg.Done()
						}()
						b.nHandlers++
					}
				}
			}()
		}
	}
}

type waiter[K comparable, V any] struct {
	ctx context.Context
	key K
	res chan *Result[V]
}

func (b *batcher[K, V]) Load(ctx context.Context, key K) (V, error) {
	waiter := b.load(ctx, key)

	select {
	case res := <-waiter:
		return res.Value, res.Err
	case <-ctx.Done():
		var v V
		return v, ctx.Err()
	}
}

func (b *batcher[K, V]) load(ctx context.Context, key K) <-chan *Result[V] {
	b.mu.Lock()
	defer func() {
		b.mu.Unlock()
	}()

	c := make(chan *Result[V], 1)

	if b.shutdown {
		c <- &Result[V]{Err: errors.New("batcher: shut down")}

		return c
	}

	b.pending = append(b.pending, waiter[K, V]{ctx: ctx, key: key, res: c})

	if b.nHandlers < b.opt.MaxHandlers {
		batch := b.nextBatch()
		if batch != nil {
			b.wg.Add(1)
			go func() {
				b.callHandlers(batch)
				b.wg.Done()
			}()
			b.nHandlers++
		}
	}

	return c
}

func (b *batcher[K, V]) nextBatch() []waiter[K, V] {
	now := time.Now()

	if b.ticker.IsZero() {
		b.ticker = now
	}

	if len(b.pending) < b.opt.MinBatchSize || now.Sub(b.ticker).Milliseconds() < b.opt.Ticker.Milliseconds() {
		return nil
	}

	defer func() {
		b.ticker = now
	}()

	// Send all
	if b.opt.MaxBatchSize == 0 || len(b.pending) <= b.opt.MaxBatchSize {
		batch := b.pending
		b.pending = nil

		return batch
	}

	batch := make([]waiter[K, V], 0, len(b.pending))
	for _, msg := range b.pending {
		if b.opt.MaxBatchSize > 0 && len(batch)+1 > b.opt.MaxBatchSize {
			break
		}

		batch = append(batch, msg)
	}
	b.pending = b.pending[len(batch):]

	return batch
}

func (b *batcher[K, V]) callHandlers(batch []waiter[K, V]) {
	for batch != nil {
		ctx := context.Background()
		if len(batch) > 0 {
			ctx = batch[0].ctx
		}

		keys := make([]K, 0, len(batch))
		for _, item := range batch {
			keys = append(keys, item.key)
		}

		items := b.handler(ctx, keys)
		if len(keys) != len(items) {
			err := &Result[V]{Err: fmt.Errorf(`
				The batch function supplied did not return an array of responses
				the same length as the array of keys.

				Keys:
				%v

				Values:
				%v
			`, keys, items)}

			for _, item := range batch {
				item.res <- err
			}
		} else {
			for i := range batch {
				batch[i].res <- items[i]
			}
		}

		b.mu.Lock()
		batch = b.nextBatch()
		if batch == nil {
			b.nHandlers--
		}
		b.mu.Unlock()
	}
}

func (b *batcher[K, V]) Shutdown() {
	b.mu.Lock()
	b.shutdown = true
	b.cancel()
	b.mu.Unlock()

	b.wg.Wait()
}
