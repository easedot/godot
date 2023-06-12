package godot

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log"
)

type RedisCache struct {
	redis *redis.Client
}

func NewRedisCache(client *redis.Client) DotCache {
	dot := RedisCache{redis: client}
	return &dot
}

func (r *RedisCache) Push(ctx context.Context, key string, values interface{}) {
	log.Println("Push to redis", key, values)
	v, err := r.redis.LPush(ctx, key, values).Result()
	if err != nil {
		log.Println("Push retryJob", err, v)
	}
}

func (r *RedisCache) BulkPush(ctx context.Context, key string, values []interface{}) {
	log.Println("Push to redis", key, "test lpush")
	pipe := r.redis.Pipeline()
	for _, v := range values {
		v, err := pipe.LPush(ctx, key, v).Result()
		if err != nil {
			log.Println("Push retryJob", err, v)
		}
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Println("Bulk Push retryJob", err)
	}

}
func (r *RedisCache) BlockPop(ctx context.Context, queue ...string) (string, error) {
	job, err := r.redis.BRPop(ctx, FetchTimeout, queue...).Result()
	if err != nil {
		return "", err
	} else {
		return job[1], nil
	}

}

func (r *RedisCache) TimeAdd(ctx context.Context, time int64, key string, values interface{}) {
	score := float64(time)
	value := redis.Z{Score: score, Member: values}
	err := r.redis.ZAdd(ctx, key, value).Err()
	if err != nil {
		log.Println(err)
	}
}

func (r *RedisCache) TimeQuery(ctx context.Context, queue string) ([]string, error) {
	op := redis.ZRangeBy{
		Min:    "-inf",
		Max:    NowTimeStamp(),
		Offset: 0,
		Count:  1,
	}
	jobs, err := r.redis.ZRangeByScore(ctx, queue, &op).Result()
	return jobs, err
}
func (r *RedisCache) TimeRem(ctx context.Context, queue, job string) (int64, error) {
	return r.redis.ZRem(ctx, queue, job).Result()
}
