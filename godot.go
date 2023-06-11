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

const ScheduleQueue = "schedule"
const RetryQueue = "retry"

const (
	DefaultMaxRetry = 25
	LAYOUT          = "2006-01-02 15:04:05 UTC"
	FetchTimeout    = 2 * time.Second
	PollInterval    = 2
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
		//each dot worker on routine
		go func(i int) {
			for job := range d.jobs {
				log.Printf("DOT[%d] Process job...\n", i)
				d.processWorker(job)
			}
			defer d.wg.Done()
		}(i)
	}
}

func (d *Dot) processWorker(job string) {
	jobData := DotData{}
	err := jsoniter.Unmarshal([]byte(job), &jobData)
	if err != nil {
		log.Println(err)
	}
	workerName := jobData.Class
	doter, exists := doters[workerName]
	if !exists {
		log.Printf("[%s] Not find register [%s]:\n", jobData.Jid, workerName)
		d.processJobError(jobData, nil)
	}
	option, _ := options[workerName]

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[%s] Panic to retry error:%s\n", jobData.Jid, r)
			d.processJobError(jobData, &option)
		}
	}()
	err = doter.Run(jobData.Args...)
	if err != nil {
		log.Printf("[%s] Error to retry error:%s\n", jobData.Jid, err)
		d.processJobError(jobData, &option)
	}
}

func (d *Dot) processJobError(jobData DotData, option *Doter) {
	retryCount := DefaultMaxRetry
	retry := true
	if option != nil && option.Retry {
		retry = option.Retry
		if option.RetryCount > 0 {
			retryCount = option.RetryCount
		}
	}

	if retry {
		log.Printf("[%s] Retry count:[%d]  retry limit:[%d]\n", jobData.Jid, jobData.RetryCount, option.RetryCount)
		if jobData.RetryCount < retryCount {
			jobData.SetRetryInfo()
			enqueue, _ := jsoniter.Marshal(jobData)
			log.Printf("[%s] Retry job at:[%d]\n", jobData.Jid, jobData.At)
			d.retry.cache.TimeAdd(jobData.At, d.retry.queue.Name, string(enqueue))
		} else {
			log.Printf("[%s] Dead job...\n", jobData.Jid)
		}
	} else {
		log.Printf("[%s] Dead job...\n", jobData.Jid)
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
				job, err := q.cache.BlockPop(queue...)
				if err == redis.Nil {
					log.Printf("Fetch queue:%s job:[]\n", queue)
				} else if err != nil {
					log.Printf("Fetch queue:%s error:[%s]\n ", queue, err)
					q.idl <- true
				} else {
					log.Printf("Fetch queue:%s\n job:[%s]\n", queue, job)
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
	poll := PollInterval*rand.Intn(10) + PollInterval/2
	ticker := time.NewTicker(time.Duration(poll) * time.Second)
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
	log.Printf("Fetch queue:[%s] job:%s \n", s.queue.Name, jobs)
	for _, job := range jobs {
		log.Println(job)
		//if not ticker
		_, err := s.cache.TimeRem(s.queue.Name, job)
		if err == nil {
			s.jobs <- job
		}
	}
}
func (s *ScheduleJob) Stop() {
	log.Printf("Stop fetch job from:[%s]\n", s.queue.Name)
	close(s.quit)
}

type GoDot struct {
	cache       DotCache
	retryJob    *ScheduleJob
	scheduleJob *ScheduleJob
	queueJob    *QueueJob
	dot         *Dot
}

func NewGoDot(client *redis.Client, queues []Queue, maxDots int) *GoDot {
	cache := NewRedisCache(client)
	jobs := make(chan string)

	retryJob := NewScheduleJob(RetryQueue, cache, jobs)
	scheduleJob := NewScheduleJob(ScheduleQueue, cache, jobs)

	queueJob := NewQueueJob(queues, cache, jobs)

	dots := NewDot(maxDots)
	dots.WaitJob(jobs, retryJob)

	godot := GoDot{
		cache:       cache,
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

type Client struct {
	cache DotCache
}

func NewGoDotCli(client *redis.Client) *Client {
	cache := NewRedisCache(client)
	godot := Client{
		cache: cache,
	}
	return &godot

}

func (d *Client) Run(className string, args ...interface{}) {
	runData, err := NewDotData(className)
	if err != nil {
		log.Println("Init run data error:", err)
	}
	runData.Args = args
	enqueue, err := jsoniter.Marshal(runData)
	if err != nil {
		log.Println("Enqueue json marshal retryJob:", err)
	}
	d.cache.Push(runData.Queue, string(enqueue))
}

func (d *Client) RunAt(at int64, className string, args ...interface{}) {
	runData, err := NewDotData(className)
	if err != nil {
		log.Println("Init run data error:", err)
	}
	runData.Args = args
	runData.Queue = ScheduleQueue //d.scheduleJob.queue.Name
	enqueue, err := jsoniter.Marshal(runData)
	if err != nil {
		log.Println("Enqueue json marshal retryJob:", err)
	}
	d.cache.TimeAdd(at, runData.Queue, string(enqueue))
}
