package main

import (
	"context"
	"fmt"
	"github.com/easedot/godot"
	"github.com/redis/go-redis/v9"
)

func main() {

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	ctx := context.Background()
	pong, err := client.Ping(ctx).Result()
	fmt.Println(pong, err)
	gdc := godot.NewGoDotCli(client)
	for i := 0; i < 50000; i++ {
		gdc.Run(ctx, "defaultDoter", "test_at")

		//gdc.Run(doters.TestJob, "test_at") //for test panic

		//gdc.Run(ctx, doters.TestJob, i, fmt.Sprintf("task index:%d ", i), i)

	}
}
