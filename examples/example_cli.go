package main

import (
	"context"
	"flag"
	"github.com/easedot/godot"
	"github.com/easedot/godot/doters"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

var (
	mj = flag.Int("m", 100, "max jobs")
)

func main() {
	flag.Parse()
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Init redis error:%s", err)
	}
	gdc := godot.NewGoDotCli(client)
	start := time.Now()
	for i := 0; i < *mj; i++ {
		//gdc.Run(ctx, godot.DefaultDoter, "test_at")

		gdc.Run(ctx, doters.TestDoter, "test_at") //for test panic

		//gdc.Run(ctx, doters.TestJob, i, fmt.Sprintf("task index:%d ", i), i)

	}
	log.Printf("Span(s):%f", time.Now().Sub(start).Seconds())
}
