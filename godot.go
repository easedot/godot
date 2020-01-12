package godot

import (
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	jsoniter "github.com/json-iterator/go"
)

var (
	fetchTimeout = 2 * time.Second
	pollInterval = 2
)

type Dot struct {
	retry   *ScheduleJob
	jobs    chan string
	wg      sync.WaitGroup
	maxDots int
}

func NewDot(maxDots int) *Dot {
	dot := Dot{maxDots: maxDots}
	return &dot
}

func (d *Dot) WaitJob(jobChan chan string, retry *ScheduleJob) {
	//d.jobs = make(chan Task, d.maxDots)
	//无缓冲通道安全，没有响应的阻塞不会丢失
	d.jobs = jobChan //make(chan string)
	d.retry = retry
	d.wg.Add(d.maxDots)
	for i := 0; i < d.maxDots; i++ {
		go func() {
			for job := range d.jobs {
				log.Println("Get job string", job)
				jobData := DotData{}
				err := jsoniter.Unmarshal([]byte(job), &jobData)
				if err != nil {
					log.Println(err)
				}
				log.Println("Parse job string to struct:", jobData)
				doter, exists := doters[jobData.Class]
				if !exists {
					log.Println("Not find register job:", jobData.Class)
					continue
				}
				option, _ := options[jobData.Class]
				err = doter.Run(jobData.Args...)
				if err != nil {
					log.Println("Retry job begin, retry count:", jobData.RetryCount, " Max limit:", option.RetryCount)
					if option.Retry && jobData.RetryCount < option.RetryCount {
						jobData.SetRetryInfo()
						enqueue, _ := jsoniter.Marshal(jobData)
						log.Println("Retry data:", string(enqueue))
						d.retry.cache.TimeAdd(jobData.At, d.retry.queue.Name, string(enqueue))
					}
				}
			}
			d.wg.Done()
		}()
	}
}

func (d *Dot) Shutdown() {
	close(d.jobs)
	//close chan wil stop testJob for each
	//wait all testJob done then shutdown
	d.wg.Wait()
}

type QueueJob struct {
	cache      DotCache
	queues     []Queue
	queueNames []string
	idl        chan bool
	jobs       chan string
	stop       chan struct{}
}

func NewQueueJob(queues []Queue, cache DotCache, jobChan chan string) *QueueJob {
	var queueNames []string
	for _, q := range queues {
		for i := 0; i < q.Weight; i++ {
			queueNames = append(queueNames, q.Name)
		}
	}
	var stop = make(chan struct{})
	var idl = make(chan bool)
	qd := QueueJob{cache, queues, queueNames, idl, jobChan, stop}
	qd.FetchJob()
	return &qd
}

func (q *QueueJob) IsIDL() bool {
	select {
	case <-q.idl:
		return true
	default:
		return false
	}
}
func (q *QueueJob) IsStop() bool {
	select {
	case <-q.stop:
		return true
	default:
		return false
	}

}
func (q *QueueJob) Stop() {
	log.Println("Stop fetch job from:", q.queueNames)
	close(q.stop)
}

func (q *QueueJob) FetchJob() {
	go func() {
		for {
			select {
			case <-q.stop:
				return
			default:
				queue := RandQueue(q.queueNames)
				//log.Println("Queue list:", queue)
				job, err := q.cache.BlockPop(queue...)
				if err != nil {
					log.Println("Wait fetch job from ", queue, " error:", err)
					q.idl <- true
				} else {
					log.Println("Fetched job from queue:", queue)
					q.jobs <- job
				}

			}
		}
	}()
}

type ScheduleJob struct {
	cache DotCache
	queue Queue
	jobs  chan string
	quit  chan struct{}
}

func NewScheduleJob(queueName string, cache DotCache, jobChan chan string) *ScheduleJob {
	q := Queue{queueName, 1}
	quit := make(chan struct{})
	sd := ScheduleJob{cache: cache, queue: q, jobs: jobChan, quit: quit}
	sd.FetchJob()
	return &sd
}
func (s *ScheduleJob) FetchJob() {
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

func (s *ScheduleJob) fetchOnce() {
	jobs, err := s.cache.TimeQuery(s.queue.Name)
	if err != nil {
		log.Println(err)
	}
	log.Println("Fetch queue:", s.queue.Name, "Job:", jobs)
	for _, job := range jobs {
		log.Println(job)
		//if not ticker
		s.cache.TimeRem(s.queue.Name, job)
		s.jobs <- job
	}
}
func (s *ScheduleJob) Stop() {
	log.Println("Stop fetch job from:", s.queue.Name)
	close(s.quit)
}

type GoDot struct {
	retryJob    *ScheduleJob
	scheduleJob *ScheduleJob
	queueJob    *QueueJob
	dot         *Dot
}

func NewGoDot(client *redis.Client, queues []Queue, maxDots int) *GoDot {
	cache := NewRedisCache(client)
	jobs := make(chan string)

	retryJob := NewScheduleJob("retryJob", cache, jobs)
	scheduleJob := NewScheduleJob("scheduleJob", cache, jobs)

	queueJob := NewQueueJob(queues, cache, jobs)

	dots := NewDot(maxDots)
	dots.WaitJob(jobs, retryJob)

	godot := GoDot{
		queueJob:    queueJob,
		retryJob:    retryJob,
		scheduleJob: scheduleJob,
		dot:         dots,
	}
	return &godot
}

func (d *GoDot) WaitIdl() {
	for !d.queueJob.IsIDL() {
		//log.Println("Wait idl...")
	}
}
func (d *GoDot) WaitJob() {

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	select {
	case <-interrupt:
		log.Println("User break testJob...")
		d.Shutdown()
	}
}

func (d *GoDot) Shutdown() {
	d.queueJob.Stop()
	d.scheduleJob.Stop()
	d.retryJob.Stop()
	d.dot.Shutdown()

}

func (d *GoDot) Run(className string, args ...interface{}) {
	runData, err := NewDotData(className)
	if err != nil {
		log.Println("Init run data error:", err)
	}
	runData.Args = args
	enqueue, err := jsoniter.Marshal(runData)
	if err != nil {
		log.Println("Enqueue json marshal retryJob:", err)
	}
	d.queueJob.cache.Push(runData.Queue, string(enqueue))
}

func (d *GoDot) RunAt(at int64, className string, args ...interface{}) {
	runData, err := NewDotData(className)
	if err != nil {
		log.Println("Init run data error:", err)
	}
	runData.Args = args
	runData.Queue = d.scheduleJob.queue.Name
	enqueue, err := jsoniter.Marshal(runData)
	if err != nil {
		log.Println("Enqueue json marshal retryJob:", err)
	}
	d.queueJob.cache.TimeAdd(at, runData.Queue, string(enqueue))
}
