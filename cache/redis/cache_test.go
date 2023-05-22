package redis

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/eaglemoor/batcher"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

type User struct {
	ID       int
	UserName string
}

var redisClient redis.UniversalClient

func init() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
}

func batch[K comparable, V any](name string, withCache bool, bfn func(context.Context, []K) []*batcher.Result[K, V]) *batcher.Batcher[K, V] {
	opts := []batcher.Option[K, V]{}
	if withCache {
		c := New[K, V](name, redisClient, 0)
		opts = append(opts, batcher.WithCache[K, V](c))
	}

	return batcher.New[K, V](bfn, opts...)
}

func initRedisData(t *testing.T, name string) {
	t.Helper()

	ctx := context.Background()

	_ = redisClient.FlushAll(ctx).Err()
}

func TestRedisCache(t *testing.T) {
	initRedisData(t, "users")

	calls := make([][]int, 0, 10)
	var mu sync.Mutex
	b := batch[int, User]("users", true, func(ctx context.Context, keys []int) []*batcher.Result[int, User] {
		mu.Lock()
		calls = append(calls, keys)
		mu.Unlock()

		result := make([]*batcher.Result[int, User], 0, len(keys))
		for _, k := range keys {
			result = append(result, &batcher.Result[int, User]{
				Key: k,
				Value: User{
					ID:       k,
					UserName: fmt.Sprintf("User #%d", k),
				},
			})
		}

		return result
	})

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
