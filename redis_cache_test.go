package godot

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedis(t *testing.T) {
	ctx := context.Background()
	var client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	pong, err := client.Ping(ctx).Result()
	fmt.Println(pong, err)
	cache := NewRedisCache(client)

	t.Run("TestZAdd", func(t *testing.T) {
		key := "test_zadd_key"
		value := "test_zadd_value"

		spanBefore := -10 * time.Second
		spanAfter := 3 * time.Second
		before := time.Now().Add(spanBefore).Unix()
		fmt.Println("before", before)
		now := time.Now().Unix()
		fmt.Println("now", now)
		after := time.Now().Add(spanAfter).Unix()
		fmt.Println("after", after)
		cache.TimeAdd(ctx, now, key, value)

		op := redis.ZRangeBy{
			Min:    "-inf",
			Max:    fmt.Sprintf("%d", before),
			Offset: 0,
			Count:  1,
		}
		//r := client.ZRangeByScoreWithScores(key, &op).String()
		//fmt.Println(fmt.Sprintf("%s", r), r)

		op.Max = fmt.Sprintf("%d", after)
		f := client.ZRangeByScoreWithScores(ctx, key, &op).String()
		fmt.Println(fmt.Sprintf("%s", f), err)
	})
}
