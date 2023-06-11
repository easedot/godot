package main

import (
	"fmt"
	"github.com/easedot/godot"
	_ "github.com/easedot/godot/doters"
	"github.com/go-redis/redis/v7"
)

func main() {
	var queues = []godot.Queue{
		{Name: "Work1", Weight: 3},
		{Name: "Work2", Weight: 2},
		{Name: "Work3", Weight: 1},
		{Name: "default", Weight: 1},
	}

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	godotSRV := godot.NewGoDot(client, queues, 100)
	defer godotSRV.WaitJob()
}
