package load

import (
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"

	"github.com/easedot/godot"
)

var queues = []godot.Queue{
	{Name: "Work1", Weight: 3},
	{Name: "Work2", Weight: 2},
	{Name: "Work3", Weight: 1},
	{Name: "default", Weight: 1},
}

var client = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
})

var gt *godot.GoDot

func init() {
	for i := 0; i < 2; i++ {
		waits[i] = 0
	}
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	gt = godot.NewGoDot(client, queues, 100)

}
func TestLoad(t *testing.T) {
	t.Run("LoadDot", func(t *testing.T) {
		for i, task := range waits {
			gt.Run(TestJob, i, fmt.Sprintf("task index:%d task:%d", i, task), i)
		}
		time.Sleep(1 * time.Minute)
		//gt.WaitJob()
		//gt.WaitIdl()
		//gt.Shutdown()
		for i, task := range waits {
			if (i % 2) == 1 {
				if task != 2 {
					t.Error("Task exec error index:", i, " value:", task)
				}
			} else {
				if task != 1 {
					t.Error("Task exec error index:", i, " value:", task)
				}
			}

		}
	})
}

var (
	runTask = []struct {
		name string
		fun  func() error
	}{
		{"runByName", load},
	}
)

func load() error {
	//index := args[0]
	log.Println("xxx")
	//gt.Run(TestJob, index, fmt.Sprintf("task index:%d ", index), index)
	return nil
}
func BenchmarkLoad(b *testing.B) {
	fmt.Println("NumCPU:", runtime.NumCPU())
	//runtime.GOMAXPROCS(runtime.NumCPU())
	for i := 1; i < 100; i++ {
		gt.Run(TestJob, i, fmt.Sprintf("task index:%d ", i), i)
	}
	gt.WaitIdl()
	gt.Shutdown()
	time.Sleep(30 * time.Second)
}
