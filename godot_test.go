package godot

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
)

var queues =[]Queue{
	{Name:"Work1",Weight:3},
	{Name:"Work2",Weight:2},
	{Name:"Work3",Weight:1},
}

type testJob struct {
	Name string
	Execed bool
}
func (job *testJob) Run(){
	job.Execed = true
	fmt.Println(job.Name)
	time.Sleep(time.Second)
}

func TestDot(t *testing.T){
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)

	t.Run("NewDot", func(t *testing.T) {
		dots:= NewDot(10)
		//defer dots.WaitJob()

		for i:=0; i<100; i++{
			d:= testJob{
				Name: fmt.Sprintf("Job:%d",i),
				Execed:false,
			}
			//fmt.Println(d)
			dots.Run(&d)
			if want,got:=true,d.Execed;want==got{
				t.Errorf("want %t got %t",want,got)
			}
		}
		time.Sleep(60*time.Second)
		dots.Shutdown()
	})
	t.Run("NewGoDot", func(t *testing.T) {
		godot:=NewGoDot(client,queues)
		//defer godot.WaitJob()

		for i:=0; i<100; i++{
			d:= testJob{
				Name: fmt.Sprintf("Job:%d",i),
				Execed:false,
			}
			//fmt.Println(d)
			godot.Run(&d)
			if want,got:=true,d.Execed;want==got{
				t.Errorf("want %t got %t",want,got)
			}
		}
		time.Sleep(60*time.Second)
		godot.Shutdown()

	})

}