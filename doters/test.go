package doters

import (
	"fmt"
	"github.com/easedot/godot"
	"time"
)

type testDoter struct {
	godot.Doter
}

func init() {
	doter := testDoter{
		Doter: godot.Doter{Queue: "Work1", Retry: false, RetryCount: 5},
	}
	godot.Register("testDoter", doter)
}

func (d *testDoter) Run(args ...interface{}) {
	fmt.Println("testJob")
	time.Sleep(time.Second)
}
