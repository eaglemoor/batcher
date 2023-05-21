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

type Result[V any] struct {
	Value V
	Err   error
}

// New create new batcher
func New[K comparable, V any](handler func(ctx context.Context, keys []K) []*Result[V], opts ...Option[K, V]) *Batcher[K, V] {
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

	hwrap := wrapHandler(o.Cache, handler)

	b := &Batcher[K, V]{
		ctx:     ctx,
		opt:     o,
		handler: hwrap.Handle,
		pending: make([]waiter[K, V], 0, o.MaxBatchSize),
		cancel:  cancel,
	}
	go b.batchTicker()

	return b
}

type Batcher[K comparable, V any] struct {
	ctx context.Context

	opt     *opt[K, V]
	handler func(context.Context, []K) []*Result[V]

	pending []waiter[K, V]
	mu      sync.Mutex

	nHandlers int
	wg        sync.WaitGroup

	cancel   func()
	shutdown bool
}

// batchTicker start b.runBatch every b.opt.Ticker time for process stuck data
func (b *Batcher[K, V]) batchTicker() {
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
	res chan *Result[V]
}

// Load load data with key and return value and error
func (b *Batcher[K, V]) Load(ctx context.Context, key K) (V, error) {
	if b.ctx == nil {
		var v V

		return v, ErrBatcherNotInit
	}

	ctx, cancel := b.context(ctx)
	defer cancel()

	waiter := b.load(ctx, key)

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

	ctx, cancel := b.context(ctx)
	defer cancel()

	waiterList := make(map[K]<-chan *Result[V], len(keys))
	var ok bool
	for _, key := range keys {
		if _, ok = waiterList[key]; ok {
			continue
		}

		waiterList[key] = b.load(ctx, key)
	}

	// Need read all channel
	for key, wt := range waiterList {
		select {
		case res := <-wt:
			if res.Err != nil {
				errors[key] = res.Err
			} else {
				data[key] = res.Value
			}
		case <-ctx.Done():
			errors[key] = ctx.Err()
		}
	}

	return data, errors
}

func (b *Batcher[K, V]) load(ctx context.Context, key K) <-chan *Result[V] {
	b.mu.Lock()
	defer b.mu.Unlock()

	c := make(chan *Result[V], 1)

	if b.shutdown {
		c <- &Result[V]{Err: ErrShotdown}
		close(c)

		return c
	}

	b.pending = append(b.pending, waiter[K, V]{ctx: ctx, key: key, res: c})

	b.runBatch(MainBatcher)

	return c
}

// runBatch checks the number of valid threads and pending data and starts a new thread to process the data.
// b.mu must be lock before
func (b *Batcher[K, V]) runBatch(btype batcherType) {
	// If the number of threads is the maximum, we do not create a new one.
	// We do not create a batcher "TimerBatcher", because the maximum number of threads shows
	// the optimal operation of the system (all keys are taken)
	if b.nHandlers >= b.opt.MaxHandlers {
		return
	}

	batch := b.nextBatch(btype)
	if len(batch) > 0 {
		b.wg.Add(1)
		go func() {
			b.callHandlers(btype, batch)
			b.wg.Done()
		}()
		b.nHandlers++
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
				close(item.res)
			}
		} else {
			for i := range batch {
				batch[i].res <- items[i]
				close(batch[i].res)
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
			b.nHandlers--
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
