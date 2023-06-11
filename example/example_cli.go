package main

import (
	"fmt"
	"github.com/easedot/godot"
	"github.com/go-redis/redis/v7"
)

func main() {

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	gdc := godot.NewGoDotCli(client)
	for i := 0; i < 100; i++ {
		gdc.Run("defaultDoter", "test_at")

		//gdc.Run(load.TestJob, "test_at") //for test panic

		//gdc.Run(load.TestJob, i, fmt.Sprintf("task index:%d ", i), i)

	}
}
