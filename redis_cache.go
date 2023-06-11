package godot

import (
	"log"

	"github.com/go-redis/redis/v7"
)

type RedisCache struct {
	redis *redis.Client
}

func NewRedisCache(client *redis.Client) DotCache {
	dot := RedisCache{redis: client}
	return &dot
}

func (r *RedisCache) Push(key string, values interface{}) {
	log.Println("Push to redis", key, values)
	v, err := r.redis.LPush(key, values).Result()
	if err != nil {
		log.Println("Push retryJob", err, v)
	}
}

func (r *RedisCache) BulkPush(key string, values []interface{}) {
	log.Println("Push to redis", key, "test lpush")
	pipe := r.redis.Pipeline()
	for _, v := range values {
		v, err := pipe.LPush(key, v).Result()
		if err != nil {
			log.Println("Push retryJob", err, v)
		}
	}
	_, err := pipe.Exec()
	if err != nil {
		log.Println("Bulk Push retryJob", err)
	}

}
func (r *RedisCache) BlockPop(queue ...string) (string, error) {
	job, err := r.redis.BRPop(FetchTimeout, queue...).Result()
	if err != nil {
		return "", err
	} else {
		return job[1], nil
	}

}

func (r *RedisCache) TimeAdd(time int64, key string, values interface{}) {
	score := float64(time)
	value := redis.Z{Score: score, Member: values}
	err := r.redis.ZAdd(key, &value).Err()
	if err != nil {
		log.Println(err)
	}
}

func (r *RedisCache) TimeQuery(queue string) ([]string, error) {
	op := redis.ZRangeBy{
		Min:    "-inf",
		Max:    NowTimeStamp(),
		Offset: 0,
		Count:  1,
	}
	jobs, err := r.redis.ZRangeByScore(queue, &op).Result()
	return jobs, err
}
func (r *RedisCache) TimeRem(queue, job string) (int64, error) {
	return r.redis.ZRem(queue, job).Result()
}
