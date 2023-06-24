package godot

import (
	"context"
	"fmt"
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

func (r *RedisCache) LLen(ctx context.Context, key string) (int64, error) {
	return r.redis.LLen(ctx, key).Result()
}

func (r *RedisCache) HGetAll(ctx context.Context, hash string) (map[string]string, error) {
	return r.redis.HGetAll(ctx, hash).Result()
}

func (r *RedisCache) HGet(ctx context.Context, hash, key string) (string, error) {
	return r.redis.HGet(ctx, hash, key).Result()
}
func (r *RedisCache) HSet(ctx context.Context, hash string, values ...interface{}) error {
	v, err := r.redis.HSet(ctx, hash, values).Result()
	if err != nil {
		log.Printf("Hset error %s value %d", err, v)
	}
	return err
}
func (r *RedisCache) HIncrBy(ctx context.Context, hash, key string, incValue int64) error {
	v, err := r.redis.HIncrBy(ctx, hash, key, incValue).Result()
	if err != nil {
		log.Printf("Hset error %s value %d", err, v)
		return err
	}
	return err
}
func (r *RedisCache) LIndex(ctx context.Context, key string, index int64) (string, error) {
	//defer trace(fmt.Sprintf("LRange %s %d", key, index))()
	return r.redis.LIndex(ctx, key, index).Result()
}

func (r *RedisCache) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	//defer trace(fmt.Sprintf("LRange %s %d %d", key, start, stop))()
	return r.redis.LRange(ctx, key, start, stop).Result()
}
func (r *RedisCache) LTrim(ctx context.Context, key string, start, stop int64) (string, error) {
	defer trace(fmt.Sprintf("LTrim %s %d %d", key, start, stop))()
	return r.redis.LTrim(ctx, key, start, stop).Result()
}
func (r *RedisCache) LPush(ctx context.Context, key string, values interface{}) {
	v, err := r.redis.LPush(ctx, key, values).Result()
	if err != nil {
		log.Println("LPush retryJob", err, v)
	}
}
func (r *RedisCache) RPush(ctx context.Context, key string, values interface{}) {
	//defer trace(fmt.Sprintf("Rpush %s", key))()
	v, err := r.redis.RPush(ctx, key, values).Result()
	if err != nil {
		log.Println("RPush retryJob", err, v)
	}
}
func (r *RedisCache) RPop(ctx context.Context, key string) (string, error) {
	return r.redis.RPop(ctx, key).Result()
}
func (r *RedisCache) LPop(ctx context.Context, key string) (string, error) {
	return r.redis.LPop(ctx, key).Result()
}
func (r *RedisCache) BulkLPush(ctx context.Context, key string, values []interface{}) {
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
func (r *RedisCache) BlockRPop(ctx context.Context, queue ...string) (string, error) {
	job, err := r.redis.BRPop(ctx, FetchTimeout, queue...).Result()
	if err != nil {
		return "", err
	} else {
		//job[0]=key=queue name
		//job[1]=value
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
