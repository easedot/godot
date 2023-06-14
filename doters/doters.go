package doters

import (
	"github.com/easedot/godot"
	"log"
)

const TestJob = "test_job"

func init() {
	options := godot.Doter{
		Queue:      "work1",
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

	//time.Sleep(time.Second)

	//test for error retry
	//index := int(args[0].(float64))
	//if (index % 2) == 1 {
	//	return fmt.Errorf("[TestDoter] raise error for retry")
	//}
	return nil
}
