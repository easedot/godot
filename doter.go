package godot

import (
	"log"
)

var doters = make(map[string]Task)

type Task interface {
	Run(args ...interface{})
}

type Queue struct {
	Name   string
	Weight int
}

type Doter struct {
	Queue      string
	Retry      bool
	RetryCount int
}

func Register(dotType string, task Task) {
	if _, exists := doters[dotType]; exists {
		log.Println(dotType, "Dot already registered")
	}
	log.Println("Register", dotType, "task")
	doters[dotType] = task
}
