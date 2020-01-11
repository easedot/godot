package godot

import (
	"log"
)

var doters = make(map[string]Task)
var options = make(map[string]Doter)

type Task interface {
	Run(args ...interface{}) error
}

type Queue struct {
	Name   string
	Weight int
}

type Doter struct {
	Dot        *GoDot
	Queue      string
	Retry      bool
	RetryCount int
}

func (dt *Doter) Run(args ...interface{}) {
	name := getStructName(dt)
	dt.Dot.Run(name, args)
}

func Register(task Task, option Doter) {
	dotType := getStructName(task)

	if _, exists := doters[dotType]; exists {
		log.Println(dotType, "Dot already registered")
	}
	log.Println("Register", dotType, "task")
	doters[dotType] = task
	options[dotType] = option
}

func RegisterByName(taskName string, task Task, option Doter) {
	if _, exists := doters[taskName]; exists {
		log.Println(taskName, "Dot already registered")
	}
	log.Println("Register", taskName, "task")
	doters[taskName] = task
	options[taskName] = option
}
