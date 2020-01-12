package godot

import (
	"fmt"
	"log"
	"time"
)

type Task interface {
	Run(args ...interface{}) error
}

type DotCache interface {
	Push(key string, values interface{})
	BulkPush(key string, values []interface{})
	BlockPop(queue ...string) (string, error)

	TimeAdd(time int64, key string, values interface{})
	TimeQuery(queue string) ([]string, error)
	TimeRem(queue, job string) (int64, error)
}

var doters = make(map[string]Task)
var options = make(map[string]Doter)

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

type Doter struct {
	Queue      string
	Retry      bool
	RetryCount int
}

type Queue struct {
	Name   string
	Weight int
}

type DotData struct {
	Queue      string        `json:"queue,omitempty"`
	Class      string        `json:"class"`
	Args       []interface{} `json:"args"`
	Jid        string        `json:"jid"`
	EnqueuedAt string        `json:"enqueued_at"`
	DotDataOption
}

type DotDataOption struct {
	RetryCount int   `json:"retry_count,omitempty"`
	Retry      bool  `json:"retryJob,omitempty"`
	At         int64 `json:"at,omitempty"`
}

func (d *DotData) SetRetryInfo() {
	d.RetryCount += 1
	//(count * *4) + 15 + (rand(30) * (count + 1))
	retrySpan := d.calcRetryTime(d.RetryCount)
	d.EnqueuedAt = NowTimeStamp()
	at := time.Now().Add(time.Duration(retrySpan) * time.Second).Unix()
	d.At = at
}
func (d *DotData) calcRetryTime(count int) int {
	//span := int(math.Pow(float64(count), 4)) + 15 + (rand.Intn(30) * (count + 1))
	span := 30 //int(math.Pow(float64(count), 2)) + 5
	return span
}
func NewDotData(className string) (*DotData, error) {
	_, exists := doters[className]
	if !exists {
		log.Println()
		return nil, fmt.Errorf("not find %s registed", className)
	}
	option := options[className]
	runData := DotData{
		Queue:      option.Queue,
		Class:      className,
		Jid:        generateJid(),
		EnqueuedAt: NowTimeStamp(),
		//DotDataOption: DotDataOption{
		//	RetryCount: option.RetryCount,
		//	Retry:      option.Retry,
		//},
	}
	return &runData, nil
}
