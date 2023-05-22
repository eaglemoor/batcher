package batcher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBatchLoad_Ok(t *testing.T) {
	handlerFn := func(ctx context.Context, keys []string) []*Result[string, string] {
		result := make([]*Result[string, string], 0, len(keys))
		for _, key := range keys {
			result = append(result, &Result[string, string]{Key: key, Value: "value_" + key})
		}

		return result
	}

	batcher := New(handlerFn)
	item, err := batcher.Load(context.Background(), "val1")
	assert.NoError(t, err)
	assert.Equal(t, "value_val1", item)
}

func TestBatchLoad_PanicRecover(t *testing.T) {
	handlerFn := func(ctx context.Context, keys []string) []*Result[string, string] {
		panic(123)
		result := make([]*Result[string, string], 0, len(keys))
		for _, key := range keys {
			result = append(result, &Result[string, string]{Key: key, Value: "value_" + key})
		}

		return result
	}

	batcher := New(handlerFn)
	item, err := batcher.Load(context.Background(), "val1")
	assert.ErrorIs(t, err, ErrPanicRecover)
	assert.Equal(t, "", item)
}

func TestBatchLoadMany_Ok(t *testing.T) {
	calls := make([][]string, 0, 10)
	m := sync.Mutex{}

	handlerFn := func(ctx context.Context, keys []string) []*Result[string, string] {
		m.Lock()
		calls = append(calls, keys)
		m.Unlock()

		result := make([]*Result[string, string], 0, len(keys))
		for _, key := range keys {
			raw := &Result[string, string]{Key: key}

			if !strings.HasPrefix(key, "err") {
				raw.Value = "value_" + key
			} else {
				raw.Err = fmt.Errorf("err_%s", key)
			}

			result = append(result, raw)
		}

		return result
	}

	batcher := New(handlerFn)

	t.Run("simple", func(t *testing.T) {
		items, errs := batcher.LoadMany(context.Background(), "val1", "val2", "val3")
		assert.Empty(t, errs)
		assert.Equal(t, map[string]string{"val1": "value_val1", "val2": "value_val2", "val3": "value_val3"}, items)

		// TODO need to debug
		assert.Equal(t, [][]string{{"val1"}, {"val2", "val3"}}, calls)
	})

	t.Run("double keys", func(t *testing.T) {
		items, errs := batcher.LoadMany(context.Background(), "val2", "val2", "val3")
		assert.Empty(t, errs)
		assert.Equal(t, map[string]string{"val2": "value_val2", "val3": "value_val3"}, items)

		// TODO need to debug
		assert.Equal(t, [][]string{{"val1"}, {"val2", "val3"}, {"val2"}, {"val3"}}, calls)
	})

	t.Run("with errors", func(t *testing.T) {
		items, errs := batcher.LoadMany(context.Background(), "val4", "err1")
		assert.Equal(t, map[string]error{"err1": errors.New("err_err1")}, errs)
		assert.Equal(t, map[string]string{"val4": "value_val4"}, items)

		// TODO need to debug
		assert.Equal(t, [][]string{{"val1"}, {"val2", "val3"}, {"val2"}, {"val3"}, {"val4"}, {"err1"}}, calls)
	})
}

func TestBatchLoad_MinBatch10(t *testing.T) {
	calls := make([][]string, 0, 10)
	var mu sync.Mutex
	handlerFn := func(ctx context.Context, keys []string) []*Result[string, string] {
		mu.Lock()
		calls = append(calls, keys)
		mu.Unlock()

		result := make([]*Result[string, string], 0, len(keys))
		for _, key := range keys {
			result = append(result, &Result[string, string]{Key: key, Value: "value_" + key})
		}

		return result
	}

	batcher := New(handlerFn, MinBatchSize[string, string](10))
	var wg sync.WaitGroup
	checker := func(key, value string) {
		defer wg.Done()

		item, err := batcher.Load(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, value, item)
	}

	startTime := time.Now()
	time.Sleep(3 * time.Millisecond) // waiting then first TimerBatch is end

	wg.Add(3)
	go checker("val1", "value_val1")
	go checker("val2", "value_val2")
	go checker("val3", "value_val3")
	wg.Wait()

	for _, row := range calls {
		sort.Strings(row)
	}

	assert.Equal(t, [][]string{{"val1", "val2", "val3"}}, calls)
	assert.GreaterOrEqual(t, time.Since(startTime).Milliseconds(), int64(17)) // batch >= 17ms
	assert.Less(t, time.Since(startTime).Milliseconds(), int64(20))           // batch < 17ms
}

func TestBatch_Timeout_ContextDeadlineExceeded(t *testing.T) {
	handlerFn := func(ctx context.Context, keys []string) []*Result[string, string] {
		result := make([]*Result[string, string], 0, len(keys))
		for _, key := range keys {
			result = append(result, &Result[string, string]{Key: key, Value: "value_" + key})
		}

		time.Sleep(time.Second)

		return result
	}

	batcher := New(handlerFn, Timeout[string, string](time.Millisecond*50))

	start := time.Now()
	ctx := context.Background()
	data, err := batcher.Load(ctx, "test1")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, "", data)
	assert.LessOrEqual(t, time.Since(start).Milliseconds(), int64(60))
}

type ctxTestKey string

func TestBatch_ContextValue(t *testing.T) {
	handlerFn := func(ctx context.Context, keys []string) []*Result[string, string] {
		ctxValue := ctx.Value(ctxTestKey("test"))

		result := make([]*Result[string, string], 0, len(keys))
		for _, key := range keys {
			result = append(result, &Result[string, string]{Key: key, Value: fmt.Sprintf("value_%s_%v", key, ctxValue)})
		}

		return result
	}

	batcher := New(handlerFn, Timeout[string, string](time.Millisecond*50))

	start := time.Now()
	ctx := context.WithValue(context.Background(), ctxTestKey("test"), "ctxvar")
	data, err := batcher.Load(ctx, "test1")
	assert.NoError(t, err)
	assert.Equal(t, "value_test1_ctxvar", data)
	assert.LessOrEqual(t, time.Since(start).Milliseconds(), int64(60))
}

func TestNotInitBatch(t *testing.T) {
	b := &Batcher[string, string]{}

	ctx := context.Background()
	var1, err1 := b.Load(ctx, "key1")
	assert.ErrorIs(t, err1, ErrBatcherNotInit)
	assert.Empty(t, var1)

	var2, err2 := b.LoadMany(ctx, "key1", "key2")
	for _, key := range []string{"key1", "key2"} {
		assert.ErrorIs(t, err2[key], ErrBatcherNotInit)
	}
	assert.Empty(t, var2)

	b.shutdown = false
	b.Shutdown()
	assert.False(t, b.shutdown)

	assert.Len(t, b.pending, 0)
}

func TestShotdown_Wait(t *testing.T) {
	handlerFn := func(ctx context.Context, keys []string) []*Result[string, string] {
		result := make([]*Result[string, string], 0, len(keys))
		for _, key := range keys {
			result = append(result, &Result[string, string]{Key: key, Value: fmt.Sprintf("value_%s", key)})
		}

		time.Sleep(time.Millisecond * 200)

		return result
	}

	b := New(handlerFn)

	start := time.Now()
	val1, err1 := b.Load(context.Background(), "test1")
	b.Shutdown()

	assert.NoError(t, err1)
	assert.Equal(t, "value_test1", val1)
	assert.GreaterOrEqual(t, time.Since(start).Milliseconds(), int64(200))

	val2, err2 := b.Load(context.Background(), "test2")
	assert.ErrorIs(t, err2, ErrShotdown)
	assert.Empty(t, val2)
}

// ---- Betch -----

func batcherForBench(b *testing.B, opts ...Option[string, string]) (*Batcher[string, string], *map[int]int) {
	b.Helper()

	calls := make(map[int]int, 100)
	var m sync.Mutex

	handlerFn := func(ctx context.Context, keys []string) []*Result[string, string] {
		m.Lock()
		calls[len(keys)]++
		m.Unlock()

		// b.Logf("bench keys: %d\n", len(keys))

		result := make([]*Result[string, string], 0, len(keys))
		for _, key := range keys {
			result = append(result, &Result[string, string]{Key: key, Value: "value_" + key})
		}

		time.Sleep(60 * time.Millisecond)

		return result
	}

	return New(handlerFn, opts...), &calls
}

func BenchmarkBatcher_load(b *testing.B) {
	batcher, calls := batcherForBench(b, MaxBatcher[string, string](100), MaxBatchSize[string, string](100), MinBatchSize[string, string](20))
	ctx := context.Background()
	b.ResetTimer()

	res := make(chan *Result[string, string], 1)
	defer close(res)

	for i := 0; i < b.N; i++ {
		batcher.load(ctx, strconv.Itoa(i), res)
	}

	batcher.Shutdown()

	fmt.Println(*calls)
}
