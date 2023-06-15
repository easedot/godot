package main

import (
	"context"
	"github.com/easedot/godot"
	_ "github.com/easedot/godot/doters"
	"github.com/redis/go-redis/v9"
	"log"
)

func main() {
	var queues = []godot.Queue{
		//{Name: "work1", Weight: 3},
		//{Name: "work2", Weight: 2},
		//{Name: "work3", Weight: 1},
		{Name: "default", Weight: 1},
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Init redis error:%s", err)
	}
	godotSRV := godot.NewGoDot(ctx, client, queues, 1000)
	defer godotSRV.WaitJob()
}
