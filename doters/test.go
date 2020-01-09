package godot

import (
	"fmt"
	"time"

	"github.com/easedot/godot"
)

//func init() {
//	doter := testDoter{
//		Doter: godot.Doter{Queue: "Work1", Retry: false, RetryCount: 5},
//	}
//	godot.Register1(doter)
//}

type testDoter struct {
	godot.Doter
	Name string
}

func NewTestJob(name string) *testDoter {
	d := testDoter{
		Doter: godot.Doter{Queue: "Work1", Retry: false, RetryCount: 5},
		Name:  name,
	}
	return &d
}

func (d testDoter) Run(args ...interface{}) error {
	fmt.Println("testJob")
	time.Sleep(time.Second)
	return nil
}
