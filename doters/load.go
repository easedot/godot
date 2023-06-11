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

	//this register use reflect gen name
	//godot.Register(doter, options)

	//this register use const name
	godot.RegisterByName(TestJob, doter, options)
}

type testDoter struct {
	godot.Doter
}

func (d testDoter) Run(args ...interface{}) error {
	log.Println("[TestDoter] Run job args:", args)
	index := int(args[0].(float64))

	waits[index] += 1
	time.Sleep(time.Second)
	if (index % 2) == 1 {
		return fmt.Errorf("[TestDoter] raise error for retry")
	}
	return nil
}
