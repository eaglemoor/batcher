package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	gredis "github.com/redis/go-redis/v9"
)

type RedisCache[K comparable, V any] struct {
	client gredis.UniversalClient
	prefix string
	ttl    time.Duration
}

func New[K comparable, V any](prefix string, client gredis.UniversalClient, ttl time.Duration) *RedisCache[K, V] {
	return &RedisCache[K, V]{client: client, prefix: prefix, ttl: ttl}
}

func (c *RedisCache[K, V]) Key(key interface{}) string {
	if ks, ok := key.(fmt.Stringer); ok {
		return fmt.Sprintf("%s_%s", c.prefix, ks.String())
	}

	return fmt.Sprintf("%s_%#v", c.prefix, key)
}

func (c *RedisCache[K, V]) Get(ctx context.Context, key K) (V, bool) {
	if c.client == nil {
		var v V
		return v, false
	}

	strkey := c.Key(key)
	var v V
	body, err := c.client.Get(ctx, strkey).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Printf("redisCache.Get can't get %s from redis: %s", strkey, err.Error())
		}

		return v, false
	}

	err = json.Unmarshal([]byte(body), &v)
	if err != nil {
		log.Printf("redisCache.Get can't unmarshal %s value %s: %s", strkey, body, err.Error())
		return v, false
	}

	return v, true
}

func (c *RedisCache[K, V]) Put(ctx context.Context, key K, value V) {
	if c.client == nil {
		return
	}

	strkey := c.Key(key)
	body, err := json.Marshal(value)
	if err != nil {
		log.Printf("redisCache.Put can't marshal %s value %v: %s", strkey, value, err.Error())
		return
	}

	if c.ttl == 0 {
		err = c.client.Set(ctx, strkey, body, 0).Err()
	} else {
		err = c.client.SetEx(ctx, strkey, body, c.ttl).Err()
	}

	if err != nil {
		log.Printf("redisCache.Put can't setex %s value %s: %s", strkey, body, err.Error())
		return
	}
}

func (c *RedisCache[K, V]) GetMany(ctx context.Context, keys []K) map[K]V {
	if c.client == nil {
		return nil
	}

	result := make(map[K]V, len(keys))

	strkey := make([]string, 0, len(keys))
	keymap := make(map[int]K, len(keys))
	for i, key := range keys {
		strkey = append(strkey, c.Key(key))
		keymap[i] = key
	}

	rows, err := c.client.MGet(ctx, strkey...).Result()
	if err != nil {
		log.Printf("redisCache.GetMany can't mget %v from redis: %s", strkey, err.Error())
		return result
	}

	for i, row := range rows {
		if row == nil {
			continue
		}

		key := keymap[i]
		var v V

		err = json.Unmarshal([]byte(row.(string)), &v)
		if err != nil {
			log.Printf("redisCache.GetMany can't unmarshal %#v: %s", row, err.Error())
			continue
		}

		result[key] = v
	}

	return result
}
