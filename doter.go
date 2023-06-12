package godot

import (
	"context"
	"fmt"
	"log"
	"time"
)

type Task interface {
	Run(args ...interface{}) error
}

type DotCache interface {
	Push(ctx context.Context, key string, values interface{})
	BulkPush(ctx context.Context, key string, values []interface{})
	BlockPop(ctx context.Context, queue ...string) (string, error)

	TimeAdd(ctx context.Context, time int64, key string, values interface{})
	TimeQuery(ctx context.Context, queue string) ([]string, error)
	TimeRem(ctx context.Context, queue, job string) (int64, error)
}

var doters = make(map[string]Task)
var options = make(map[string]Doter)

func Register(task Task, option Doter) {
	dotType := getStructName(task)

	if _, exists := doters[dotType]; exists {
		log.Printf("Worker name [%s] already registered\n", dotType)
	}
	log.Printf("Register [%s] task \n", dotType)
	doters[dotType] = task
	options[dotType] = option
}

func RegisterByName(taskName string, task Task, option Doter) {
	if _, exists := doters[taskName]; exists {
		log.Printf("Worker name [%s] already registered\n", taskName)
	}
	log.Printf("Register [%s] task \n", taskName)
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
	}
	return &runData, nil
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
