package batcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	DefaultMaxBatchSize = 100
	DefaultMinBatchSize = 1
	DefaultMaxHandlers  = 1
	DefaultTicker       = 17 * time.Millisecond
)

type batcherType int

const (
	MainBatcher batcherType = iota
	TimerBatcher
)

type Result[K comparable, V any] struct {
	Key   K
	Value V
	Err   error
}

// New create new batcher
func New[K comparable, V any](handler func(ctx context.Context, keys []K) []*Result[K, V], opts ...Option[K, V]) *Batcher[K, V] {
	o := &opt[K, V]{
		MaxBatchSize: DefaultMaxBatchSize,
		MinBatchSize: DefaultMinBatchSize,
		MaxBatcher:   DefaultMaxHandlers,
		Ticker:       DefaultTicker,
	}

	for _, oo := range opts {
		oo(o)
	}

	ctx, cancel := context.WithCancel(context.Background())

	hwrap := wrapHandler(o.Cache, handler)

	b := &Batcher[K, V]{
		ctx:     ctx,
		opt:     o,
		handler: hwrap.Handle,
		pending: make([]waiter[K, V], 0, o.MaxBatchSize),
		cancel:  cancel,
	}
	go b.runTimeBatch()

	return b
}

type Batcher[K comparable, V any] struct {
	ctx context.Context

	opt     *opt[K, V]
	handler func(context.Context, []K) []*Result[K, V]

	pending []waiter[K, V]
	mu      sync.Mutex

	batcherCount int
	wg           sync.WaitGroup

	cancel   func()
	shutdown bool
}

// runTimeBatch start b.runBatch every b.opt.Ticker time for process stuck data
func (b *Batcher[K, V]) runTimeBatch() {
	limiter := rate.NewLimiter(rate.Every(b.opt.Ticker), 1)
	for limiter.Wait(b.ctx) == nil {
		func() {
			b.mu.Lock()
			defer b.mu.Unlock()

			b.runBatch(TimerBatcher)
		}()
	}
}

type waiter[K comparable, V any] struct {
	ctx context.Context
	key K
	res chan *Result[K, V]
}

// Load load data with key and return value and error
func (b *Batcher[K, V]) Load(ctx context.Context, key K) (V, error) {
	if b.ctx == nil {
		var v V

		return v, ErrBatcherNotInit
	}

	ctx, cancel := b.context(ctx)
	defer cancel()

	waiter := make(chan *Result[K, V], 1)
	defer close(waiter)

	b.load(ctx, key, waiter)

	select {
	case res := <-waiter:
		return res.Value, res.Err
	case <-ctx.Done():
		var v V
		return v, ctx.Err()
	}
}

// LoadMany load slice data and return value and error map
func (b *Batcher[K, V]) LoadMany(ctx context.Context, keys ...K) (map[K]V, map[K]error) {
	data := make(map[K]V, len(keys))
	errors := make(map[K]error, len(keys))

	if b.ctx == nil {
		for _, key := range keys {
			errors[key] = ErrBatcherNotInit
		}

		return data, errors
	}

	if len(keys) == 0 {
		return data, errors
	}

	// remove double
	keys = scoringKey(keys)

	// wrap context
	ctx, cancel := b.context(ctx)
	defer cancel()

	// channel for read result
	waiterlist := make(chan *Result[K, V], len(keys))
	defer close(waiterlist)

	for _, key := range keys {
		b.load(ctx, key, waiterlist)
	}

	c := len(keys)

	for c > 0 {
		select {
		case <-ctx.Done():
			// enrich the result with errors
			enrichErrors(keys, data, errors, ctx.Err())

			return data, errors

		case res := <-waiterlist:
			if res.Err != nil {
				errors[res.Key] = res.Err
			} else {
				data[res.Key] = res.Value
			}
			c--
		}
	}

	return data, errors
}

func (b *Batcher[K, V]) load(ctx context.Context, key K, result chan *Result[K, V]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.shutdown {
		result <- &Result[K, V]{Key: key, Err: ErrShotdown}
	}

	b.pending = append(b.pending, waiter[K, V]{ctx: ctx, key: key, res: result})

	b.runBatch(MainBatcher)
}

// runBatch checks the number of valid threads and pending data and starts a new thread to process the data.
// b.mu must be lock before
func (b *Batcher[K, V]) runBatch(btype batcherType) {
	// If the number of threads is the maximum, we do not create a new one.
	// We do not create a batcher "TimerBatcher", because the maximum number of threads shows
	// the optimal operation of the system (all keys are taken)
	if b.batcherCount >= b.opt.MaxBatcher {
		return
	}

	batch := b.nextBatch(btype)
	if len(batch) > 0 {
		b.wg.Add(1)
		go func() {
			b.callHandlers(btype, batch)
			b.wg.Done()
		}()
		b.batcherCount++
	}
}

// nextBatch fetches data from pending.
// b.mu must be lock before
func (b *Batcher[K, V]) nextBatch(btype batcherType) (bch []waiter[K, V]) {
	if len(b.pending) < b.opt.MinBatchSize && btype != TimerBatcher {
		return nil
	}

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

// callHandlers prepare context, slice of keys and run user handler.
// After processing the batch tries to get a new data packet. If pending data is empty
func (b *Batcher[K, V]) callHandlers(btype batcherType, batch []waiter[K, V]) {
	for len(batch) > 0 {
		ctx := batch[0].ctx

		keys := make([]K, 0, len(batch))
		for _, item := range batch {
			keys = append(keys, item.key)
		}

		items := b.handler(ctx, keys)
		if len(keys) != len(items) {
			err := fmt.Errorf(`
				The batch function supplied did not return an array of responses
				the same length as the array of keys.

				Keys:
				%v

				Values:
				%v
			`, keys, items)

			for _, item := range batch {
				item.res <- &Result[K, V]{Key: item.key, Err: err}
			}
		} else {
			for i := range batch {
				batch[i].res <- items[i]
			}
		}

		b.mu.Lock()

		// At the first start TimerBatcher takes all the data from pending, the size of which is less than the minimum size batch size.
		// In order for this batcher to work according to the general rules in the future, we change its type.
		if btype == TimerBatcher {
			btype = MainBatcher
		}
		batch = b.nextBatch(btype)
		if len(batch) == 0 {
			b.batcherCount--
		}
		b.mu.Unlock()
	}
}

func (b *Batcher[K, V]) context(ctx context.Context) (context.Context, context.CancelFunc) {
	// Detache ctx if needed
	if b.opt.Timeout > 0 {
		ctx, cancel := context.WithTimeout(DetachedContext(ctx), b.opt.Timeout)
		return ctx, cancel
	}

	return context.WithCancel(ctx)
}

func (b *Batcher[K, V]) Shutdown() {
	if b.ctx == nil {
		return
	}

	b.mu.Lock()
	b.shutdown = true
	b.cancel()
	b.mu.Unlock()

	b.wg.Wait()
}
