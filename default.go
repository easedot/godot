package godot

import (
	"log"
	"time"
)

type defaultDoter struct {
	Doter
}

func init() {
	doter := defaultDoter{
		Doter{
			Queue:      "default",
			Retry:      false,
			RetryCount: 2,
		},
	}
	name := getStructName(doter)
	Register(name, doter)
}
func (d defaultDoter) Run(args ...interface{}) error {
	log.Println("Default dot run args:", args)
	time.Sleep(time.Second)
	log.Println("Default dot done ...")
	return nil
}
