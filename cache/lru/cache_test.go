package lru

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/eaglemoor/batcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type User struct {
	ID       int
	UserName string
}

func batch[K comparable, V any](name string, withCache bool, bfn func(context.Context, []K) []*batcher.Result[V]) (*batcher.Batcher[K, V], error) {
	opts := []batcher.Option[K, V]{}
	if withCache {
		c, err := New[K, V](100)
		if err != nil {
			return nil, err
		}

		opts = append(opts, batcher.WithCache[K, V](c))
	}

	return batcher.New[K, V](bfn, opts...), nil
}

func TestRedisCache(t *testing.T) {

	calls := make([][]int, 0, 10)
	var mu sync.Mutex
	b, err := batch[int, User]("users", true, func(ctx context.Context, keys []int) []*batcher.Result[User] {
		mu.Lock()
		calls = append(calls, keys)
		mu.Unlock()

		result := make([]*batcher.Result[User], 0, len(keys))
		for _, k := range keys {
			result = append(result, &batcher.Result[User]{Value: User{
				ID:       k,
				UserName: fmt.Sprintf("User #%d", k),
			}})
		}

		return result
	})

	require.NoError(t, err)

	ctx := context.Background()

	user, err := b.Load(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, User{ID: 1, UserName: "User #1"}, user)
	func() {
		mu.Lock()
		defer mu.Unlock()

		assert.Equal(t, [][]int{{1}}, calls)
	}()

	users, errs := b.LoadMany(ctx, 1, 2, 3)
	assert.Empty(t, errs)

	assert.Equal(t, map[int]User{
		1: User{1, "User #1"},
		2: User{2, "User #2"},
		3: User{3, "User #3"},
	}, users)

	// {1} - from first call
	// {1} - no, b from cache
	// {2, 3} - next call
	assert.Equal(t, [][]int{{1}, {2, 3}}, calls)
}
