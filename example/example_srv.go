package main

import (
	"context"
	"fmt"
	"github.com/easedot/godot"
	_ "github.com/easedot/godot/doters"
	"github.com/redis/go-redis/v9"
)

func main() {
	var queues = []godot.Queue{
		{Name: "work1", Weight: 3},
		{Name: "work2", Weight: 2},
		{Name: "work3", Weight: 1},
		{Name: "default", Weight: 1},
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	pong, err := client.Ping(ctx).Result()
	fmt.Println(pong, err)
	godotSRV := godot.NewGoDot(ctx, client, queues, 100)
	defer godotSRV.WaitJob()
}
