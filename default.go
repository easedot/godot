package godot

import (
	"log"
	"time"
)

func init() {
	options := Doter{
		Queue:      "default",
		Retry:      false,
		RetryCount: 2,
	}
	doter := defaultDoter{options}
	Register(doter, options)
}

type defaultDoter struct {
	Doter
}

func (d defaultDoter) Run(args ...interface{}) error {
	log.Println("DefaultDoter dot run args:", args)
	time.Sleep(time.Second)
	log.Println("DefaultDoter dot done ...")
	return nil
}
