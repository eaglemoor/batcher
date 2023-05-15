package batcher

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchLoad_Ok(t *testing.T) {
	handlerFn := func(ctx context.Context, keys []string) []*Result[string] {
		result := make([]*Result[string], 0, len(keys))
		for _, key := range keys {
			result = append(result, &Result[string]{Value: "value_" + key})
		}

		return result
	}

	batcher := New(handlerFn)
	item, err := batcher.Load(context.Background(), "val1")
	assert.NoError(t, err)
	assert.Equal(t, "value_val1", item)
}

func TestCallNumber_Ok(t *testing.T) {
	calls := make([][]string, 0, 10)
	var mu sync.Mutex
	handlerFn := func(ctx context.Context, keys []string) []*Result[string] {
		mu.Lock()
		calls = append(calls, keys)
		mu.Unlock()

		result := make([]*Result[string], 0, len(keys))
		for _, key := range keys {
			result = append(result, &Result[string]{Value: "value_" + key})
		}

		return result
	}

	batcher := New(handlerFn)
	var wg sync.WaitGroup
	checker := func(key, value string) {
		defer wg.Done()

		item, err := batcher.Load(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, value, item)
	}

	wg.Add(3)
	go checker("val1", "value_val1")
	go checker("val2", "value_val2")
	go checker("val3", "value_val3")
	wg.Wait()

	for _, row := range calls {
		sort.Strings(row)
	}

	assert.Equal(t, [][]string{{"val1", "val2", "val3"}}, calls)
}

func batcherForBench(b *testing.B, opts ...Option[string, string]) *batcher[string, string] {
	b.Helper()

	handlerFn := func(ctx context.Context, keys []string) []*Result[string] {
		b.Log(len(keys))

		result := make([]*Result[string], 0, len(keys))
		for _, key := range keys {
			result = append(result, &Result[string]{Value: "value_" + key})
		}

		return result
	}

	return New(handlerFn, MaxHandlers[string, string](10), MaxBatchSize[string, string](100))
}

func BenchmarkBatcher_load(b *testing.B) {
	batcher := batcherForBench(b, MaxHandlers[string, string](10), MaxBatchSize[string, string](100))
	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batcher.load(ctx, strconv.Itoa(i))
	}

	batcher.Shutdown()
}
