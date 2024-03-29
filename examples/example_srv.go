package main

import (
	"context"
	"flag"
	"github.com/easedot/godot"
	_ "github.com/easedot/godot/doters"
	"github.com/redis/go-redis/v9"
	"log"
)

var (
	mr = flag.Int("m", 100, "max redis cnn")
)

func main() {
	flag.Parse()

	var queues = []godot.Queue{
		{Name: "work1", Weight: 3},
		//{Name: "work2", Weight: 2},
		//{Name: "work3", Weight: 1},
		{Name: "default", Weight: 1},
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
		PoolSize: *mr,
	})
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Init redis error:%s", err)
	}

	godotSRV := godot.NewGoDot(ctx, client, queues, *mr, 6698)

	defer godotSRV.WaitJob()
}
