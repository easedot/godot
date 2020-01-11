package load

import (
	"log"
	"time"

	"github.com/easedot/godot"
)

const TestJob = "test_job"

func init() {
	options := godot.Doter{
		Queue:      "default",
		Retry:      false,
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
	log.SetPrefix(TestJob)
	log.Println(" Run args:", args)
	time.Sleep(time.Second)
	log.Println(" Done args:", args[0])
	return nil
}
