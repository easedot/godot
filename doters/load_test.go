package load

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis/v7"

	"github.com/easedot/godot"
)

var queues = []godot.Queue{
	{Name: "Work1", Weight: 3},
	{Name: "Work2", Weight: 2},
	{Name: "Work3", Weight: 1},
	{Name: "default", Weight: 1},
}
var waits = make(map[int]int)

var client = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
})

var gt *godot.GoDot

func init() {
	for i := 0; i < 100; i++ {
		waits[i] = 0
	}
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	gt = godot.NewGoDot(client, queues, 10)

}
func TestLoad(t *testing.T) {
	defer gt.WaitJob()
	t.Run("LoadDot", func(t *testing.T) {
		for i, task := range waits {
			gt.Run(TestJob, fmt.Sprintf("task index:%d task:%d", i, task), i)
		}
		gt.WaitJob()
		for i, task := range waits {
			if i != task {
				t.Error("Task exec error index:", i)
			}
		}
	})
}

var (
	runTask = []struct {
		name string
		fun  func(...interface{}) error
	}{
		{"runByName",
			func(i ...interface{}) error {
				gt.Run(TestJob, fmt.Sprintf("task index:%d", i), i)
				return nil
			},
		},
	}
)

func BenchmarkLoad(b *testing.B) {
	for _, f := range runTask {
		b.Run(f.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f.fun(i)
			}
			gt.WaitJob()
		})
	}
}
