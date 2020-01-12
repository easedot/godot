package load

import (
	"fmt"
	"log"
	"time"

	"github.com/easedot/godot"
)

var waits = make(map[int]int)

const TestJob = "test_job"

func init() {
	options := godot.Doter{
		Queue:      "default",
		Retry:      true,
		RetryCount: 2,
	}
	doter := testDoter{options}
	//godot.Register(doter, options)
	godot.RegisterByName(TestJob, doter, options)
}

type testDoter struct {
	godot.Doter
}

func (d testDoter) Run(args ...interface{}) error {
	log.Println("Run job begin args:", args)
	index := int(args[0].(float64))

	waits[index] += 1
	time.Sleep(time.Second)
	log.Println("Run job end args:", args)
	if (index % 2) == 1 {
		return fmt.Errorf("run job raise error for retry")
	}
	return nil
}
