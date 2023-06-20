package godot

import "log"

const DefaultDoter = "default_dot"

func init() {
	options := Doter{
		Queue:      "default",
		Retry:      false,
		RetryCount: 2,
	}
	doter := defaultDoter{options}
	RegisterByName(DefaultDoter, doter, options)
}

type defaultDoter struct {
	Doter
}

func (d defaultDoter) Run(args ...interface{}) error {
	log.Println("DefaultDoter dot run args:", args)
	//time.Sleep(time.Second)
	//log.Println("DefaultDoter dot done ...")
	return nil
}
