package godot

import (
	crand "crypto/rand"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

var (
	fetchTimeout = 2 * time.Second
	pollInterval = 2
)

type EnqueueData struct {
	Queue      string      `json:"queue,omitempty"`
	Class      string      `json:"class"`
	Data       interface{} `json:"data"`
	Args       interface{} `json:"args"`
	Jid        string      `json:"jid"`
	EnqueuedAt string      `json:"enqueued_at"`
	EnqueueOption
}

type EnqueueOption struct {
	RetryCount int  `json:"retry_count,omitempty"`
	Retry      bool `json:"error,omitempty"`
	At         int  `json:"at,omitempty"`
}

type Dot struct {
	error   chan string
	dots    chan string
	wg      sync.WaitGroup
	maxDots int
}

func NewDot(maxDots int) *Dot {
	dot := Dot{maxDots: maxDots}
	dot.InitDots()
	return &dot
}

func (d *Dot) InitDots() {
	//d.dots = make(chan Task, d.maxDots)
	//无缓冲通道安全，没有响应的阻塞不会丢失
	d.dots = make(chan string)
	d.error = make(chan string)
	d.wg.Add(d.maxDots)
	for i := 0; i < d.maxDots; i++ {
		go func() {
			for job := range d.dots {
				jobData := EnqueueData{}
				err := jsoniter.Unmarshal([]byte(job), &jobData)
				if err != nil {
					log.Println(err)
				}
				//fmt.Println("Pop work:", jobData)
				doter, exists := doters[jobData.Class]
				if !exists {
					doter = doters["defaultDoter"]
				}

				err = doter.Run(jobData.Args)
				if err != nil {
					log.Println("Exec job error", err)
					d.error <- job
				}
			}
			d.wg.Done()
		}()
	}
}

func (d *Dot) Run(job Task) {
	//d.dots <- job
}

func (d *Dot) Shutdown() {
	close(d.dots)
	//close chan wil stop testJob for each
	//wait all testJob done then shutdown
	d.wg.Wait()
}

type RedisDot struct {
	redis *redis.Client
	*Dot
}

func NewRedisDot(maxDots int, client *redis.Client) *RedisDot {
	d := NewDot(maxDots)
	dot := RedisDot{redis: client, Dot: d}
	return &dot
}
func (r *RedisDot) ZAdd(score float64, key string, values interface{}) {
	value := redis.Z{Score: score, Member: values}
	err := r.redis.ZAdd(key, &value).Err()
	if err != nil {
		panic(err)
	}
}
func (r *RedisDot) Push(key string, values interface{}) {
	log.Println("Push to redis", key, values)
	v, err := r.redis.LPush(key, values).Result()
	if err != nil {
		log.Println("Push error", err, v)
	}
}

func (r *RedisDot) BulkPush(key string, values []interface{}) {
	log.Println("Push to redis", key, "test lpush")
	pipe := r.redis.Pipeline()
	for _, v := range values {
		v, err := pipe.LPush(key, v).Result()
		if err != nil {
			log.Println("Push error", err, v)
		}
	}
	_, err := pipe.Exec()
	if err != nil {
		log.Println("Bulk Push error", err)
	}

}

type QueueDot struct {
	*RedisDot
	queues     []Queue
	queueNames []string
}

func NewQueueDot(queues []Queue, client *redis.Client, maxDots int) *QueueDot {
	rd := NewRedisDot(maxDots, client)
	var queueNames []string
	for _, q := range queues {
		for i := 0; i < q.Weight; i++ {
			queueNames = append(queueNames, q.Name)
		}
	}

	qd := QueueDot{rd, queues, queueNames}
	return &qd
}

func (q *QueueDot) FetchJob() {
	go func() {
		for {
			queue := q.queueWeight()
			//log.Println("Queue list:", queue)
			job, _ := q.redis.BRPop(fetchTimeout, queue...).Result()
			//log.Println("Pop from redis ", job)
			if len(job) > 1 {
				q.dots <- job[1]
			} else {
				log.Println("Wait job to run...")
			}
		}
	}()
}

func (q *QueueDot) queueWeight() []string {
	rand.Shuffle(len(q.queueNames), func(i, j int) {
		q.queueNames[i], q.queueNames[j] = q.queueNames[j], q.queueNames[i]
	})
	queue := q.unique()
	return queue
}

func (q *QueueDot) unique() []string {
	keys := make(map[string]bool)
	var list []string
	for _, entry := range q.queueNames {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

type ScheduleDot struct {
	*RedisDot
	queue Queue
}

func NewScheduleDot(queueName string, client *redis.Client) *ScheduleDot {
	q := Queue{queueName, 1}
	rd := NewRedisDot(1, client)
	sd := ScheduleDot{rd, q}
	return &sd
}
func (s *ScheduleDot) Run() {
	//todo push job to redis
	log.Println("Schedule exec push...")
}

func (s *ScheduleDot) FetchJob() {
	pollInterval = pollInterval*rand.Intn(10) + pollInterval/2
	ticker := time.NewTicker(time.Duration(pollInterval) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				s.fetchOnce()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (s *ScheduleDot) fetchOnce() {
	now := NowTimeStamp()

	op := redis.ZRangeBy{
		Min:    "-inf",
		Max:    now,
		Offset: 0,
		Count:  1,
	}
	jobs, err := s.redis.ZRangeByScore(s.queue.Name, &op).Result()
	if err != nil {
		log.Println(err)
	}
	log.Println("Fetch queue:", s.queue.Name, "Job:", jobs)
	for _, job := range jobs {
		log.Println(job)
		//if not ticker
		s.redis.ZRem(s.queue.Name, job)
		s.dots <- job
	}
}

type GoDot struct {
	retryDot    *ScheduleDot
	scheduleDot *ScheduleDot
	queueDot    *QueueDot
}

func NewGoDot(client *redis.Client, queues []Queue, maxDots int) *GoDot {
	queueDot := NewQueueDot(queues, client, maxDots)
	queueDot.FetchJob()

	retryDot := NewScheduleDot("retry", client)
	retryDot.Dot = queueDot.Dot
	retryDot.FetchJob()

	schedule := NewScheduleDot("schedule", client)
	schedule.Dot = queueDot.Dot
	schedule.FetchJob()

	godot := GoDot{
		queueDot:    queueDot,
		retryDot:    retryDot,
		scheduleDot: schedule,
	}
	return &godot
}

func (d *GoDot) WaitJob() {
	go func() {
		for job := range d.queueDot.error {
			log.Println("Retry job add:", job)
			at := time.Now().Add(10 * time.Second).Unix()
			d.retryDot.ZAdd(float64(at), d.retryDot.queue.Name, job)
		}
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	select {
	case <-interrupt:
		log.Println("User break testJob...")
		d.queueDot.Shutdown()
		d.retryDot.Shutdown()
		d.scheduleDot.Shutdown()
	}
}

func (d *GoDot) Run(job Task, args ...interface{}) {
	dotName := getStructName(job)
	Register(dotName, job)
	jobID := googleJid()

	queueName := "default"
	runData := EnqueueData{
		Queue:      queueName,
		Class:      dotName,
		Args:       args,
		Jid:        jobID,
		EnqueuedAt: NowTimeStamp(),
	}

	enqueue, err := jsoniter.Marshal(runData)
	//fmt.Println(string(enqueue))
	if err != nil {
		log.Println("Enqueue json marshal error:", err)
	}
	d.queueDot.Push(queueName, string(enqueue))

}
func (d *GoDot) RunAt(at int64, job Task, args ...interface{}) {
	dotName := getStructName(job)
	Register(dotName, job)
	jobID := googleJid()

	queueName := d.scheduleDot.queue.Name
	runData := EnqueueData{
		Queue:      queueName,
		Class:      dotName,
		Args:       args,
		Jid:        jobID,
		EnqueuedAt: NowTimeStamp(),
	}

	enqueue, err := jsoniter.Marshal(runData)
	//fmt.Println(string(enqueue))
	if err != nil {
		log.Println("Enqueue json marshal error:", err)
	}
	fat := float64(at)
	d.scheduleDot.ZAdd(fat, queueName, string(enqueue))
}
func (d *GoDot) Run1(dotName string, at int, args ...interface{}) {
	jobID := googleJid()

	//jobData, err := jsoniter.Marshal(job)
	//if err != nil {
	//	log.Println(err)
	//}
	//var jobmap map[string]interface{}
	//err = jsoniter.Unmarshal(jobData, &jobmap)
	//if err != nil {
	//	log.Println(err)
	//}

	queueName := "default"
	//if jobmap["queue"] != "" {
	//	queueName = fmt.Sprintf("%v", jobmap["Queue"])
	//}
	runData := EnqueueData{
		Queue:         queueName,
		Class:         dotName,
		Args:          args,
		Jid:           jobID,
		EnqueuedAt:    NowTimeStamp(),
		EnqueueOption: EnqueueOption{At: at},
	}

	enqueue, err := jsoniter.Marshal(runData)
	//fmt.Println(string(enqueue))
	if err != nil {
		log.Println("Enqueue json marshal error:", err)
	}
	d.queueDot.Push(queueName, string(enqueue))
}

func NowTimeStamp() string {
	now := fmt.Sprintf("%d", time.Now().Unix())
	return now
}

func getStructName(in interface{}) string {
	obj := reflect.Indirect(reflect.ValueOf(in))
	typ := obj.Type()
	jobName := typ.Name()
	return jobName
}

func generateJid() string {
	// Return 12 random bytes as 24 character hex
	b := make([]byte, 12)
	_, err := io.ReadFull(crand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

func googleJid() string {
	id, err := uuid.NewUUID()
	if err != nil {
		// handle error
	}
	return fmt.Sprintf(id.String())
}
func googleJidV2() string {
	id := uuid.New()
	return id.String()
}
