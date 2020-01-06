package godot

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

var(
	fetchTimeout = 2*time.Second
	pollInterval = 10
)

type Queue struct {
	Name string
	Weight int
}

type Task interface {
	Run()
}

type Dot struct {
	jobs chan Task
	wg sync.WaitGroup
	maxDots int
}

func NewDot(maxDots int) *Dot{
	dot:=Dot{maxDots:maxDots}
	dot.InitDots()
	return &dot
}

func (d *Dot) InitDots() {
	d.jobs=make(chan Task,d.maxDots)
	d.wg.Add(d.maxDots)
	for i := 0; i < d.maxDots; i++ {
		go func() {
			for job := range d.jobs {
				job.Run()
			}
			d.wg.Done()
		}()
	}
}

func (d *Dot)Run(job Task)  {
	d.jobs<-job
}

func (d *Dot) Shutdown(){
	close(d.jobs)
	//close chan wil stop testJob for each
	//wait all testJob done then shutdown
	d.wg.Wait()
}
func (d *Dot) WaitJob(){
	interrupt:=make( chan os.Signal,1)
	signal.Notify(interrupt,os.Interrupt)
	select{
		case <-interrupt:
			fmt.Println("User break testJob...")
			d.Shutdown()
	}
}


type RedisDot struct {
	redis *redis.Client
	*Dot
}
func NewRedisDot(maxDots int,client *redis.Client) *RedisDot{
	d:=NewDot(maxDots)
	dot:=RedisDot{redis: client, Dot:d}
	return &dot
}

type QueueDot struct {
	*RedisDot
	queue Queue
}

func NewQueueDot(queueName string,queueWeight int,client *redis.Client) *QueueDot{
	queue:=Queue{queueName,queueWeight}
	rd:=NewRedisDot(queueWeight,client)
	qd:=QueueDot{rd,queue}
	return &qd
}

func(q *QueueDot) Run() {
	//todo exec job
	fmt.Println("Queue exec ")
}
func(q *QueueDot) FetchJob(){
	go func() {
		for{
			job,_:=q.redis.BRPop(fetchTimeout,q.queue.Name).Result()
			fmt.Println(job)
			q.jobs<-q
		}
	}()
}

type ScheduleDot struct {
	*RedisDot
	queue Queue
}
func NewScheduleDot(queueName string,client *redis.Client) *ScheduleDot {
	q:=Queue{queueName,1}
	rd:=NewRedisDot(1,client)
	sd:=ScheduleDot{rd,q}
	return &sd
}
func(s *ScheduleDot) Run() {
	//todo push job to redis
	fmt.Println("Schedule exec push...")
}

func(s *ScheduleDot) FetchJob() {
	pollInterval = pollInterval* rand.Intn(10) + pollInterval/ 2
	ticker := time.NewTicker(time.Duration(pollInterval) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <- ticker.C:
				s.fetchOnce()
			case <- quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func(s *ScheduleDot) fetchOnce(){
	now:=time.Now().String()
	op := redis.ZRangeBy{
		Min:"-inf",
		Max:now,
		Offset:0,
		Count:1,
	}
	jobs, err := s.redis.ZRangeByScore(s.queue.Name, &op).Result()
	if err != nil {
		log.Println(err)
	}

	for _, job := range jobs {
		s.redis.ZRem(s.queue.Name,job)
		fmt.Println(job)
	}
	s.jobs<-s
}


type GoDot struct {
	retryDot    *ScheduleDot
	scheduleDot *ScheduleDot
	queueDots   []*QueueDot
}

func NewGoDot(client * redis.Client,queues []Queue) *GoDot{
	retryDot:=NewScheduleDot("retry",client)
	retryDot.FetchJob()

	schedule:=NewScheduleDot("schedule",client)
	schedule.FetchJob()

	var queueDots []*QueueDot
	for _,queue:=range queues{
		dot:=NewQueueDot(queue.Name,queue.Weight,client)
		queueDots=append(queueDots, dot)
		defer func(dot *QueueDot) {
			dot.WaitJob()
		}(dot)
	}
	godot:=GoDot{queueDots:queueDots,retryDot:retryDot,scheduleDot:schedule}
	return &godot
}





