package godot

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
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

func (d *Dot) WaitJob(ctx context.Context, jobChan chan string, retry *ScheduleJob) {
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
				d.processWorker(ctx, job)
			}
			defer d.wg.Done()
		}(i)
	}
}

func (d *Dot) processWorker(ctx context.Context, job string) {
	jobData := DotData{}
	err := jsoniter.Unmarshal([]byte(job), &jobData)
	if err != nil {
		log.Printf("Unmarshal json err:%s\n", err)
	}
	workerName := jobData.Class
	doter, exists := doters[workerName]
	if !exists {
		log.Printf("[%s] Not find register [%s]:\n", jobData.Jid, workerName)
		d.processJobError(ctx, jobData, nil)
		return
	}
	option, _ := options[workerName]

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[%s] Panic to retry error:%s\n", jobData.Jid, r)
			d.processJobError(ctx, jobData, &option)
		}
	}()
	err = doter.Run(jobData.Args...)
	if err != nil {
		log.Printf("[%s] Error to retry error:%s\n", jobData.Jid, err)
		d.processJobError(ctx, jobData, &option)
	}
	//todo set success count
}

func (d *Dot) processJobError(ctx context.Context, jobData DotData, option *Doter) {
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
			d.retry.cache.TimeAdd(ctx, jobData.At, d.retry.queue.Name, string(enqueue))
		} else {
			//todo set dead job count
			log.Printf("[%s] Dead job...\n", jobData.Jid)
		}
	} else {
		//todo set dead job count
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

func NewQueueJob(ctx context.Context, queues []Queue, cache DotCache, jobChan chan string, maxFetch int) *QueueJob {
	var queueNames []string
	for _, q := range queues {
		for i := 0; i < q.Weight; i++ {
			queueNames = append(queueNames, q.Name)
		}
	}
	var stop = make(chan struct{})
	var idl = make(chan bool)
	qd := QueueJob{cache, queues, queueNames, idl, jobChan, stop}
	for i := 0; i < maxFetch; i++ {
		qd.FetchJob(ctx)
	}
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
	log.Printf("Stop fetch job from:%s\n", q.queueNames)
	close(q.stop)
}

func (q *QueueJob) FetchJob(ctx context.Context) {
	go func() {
		for {
			select {
			case <-q.stop:
				return

			default:
				queue := RandQueue(q.queueNames)
				job, err := q.cache.BlockPop(ctx, queue...)
				if err == redis.Nil {
					//log.Printf("Fetch queue:%s job:[]\n", queue)
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

func NewScheduleJob(ctx context.Context, queueName string, cache DotCache, jobChan chan string) *ScheduleJob {
	q := Queue{queueName, 1}
	quit := make(chan struct{})
	sd := ScheduleJob{cache: cache, queue: q, jobs: jobChan, quit: quit}
	sd.FetchJob(ctx)
	return &sd
}
func (s *ScheduleJob) FetchJob(ctx context.Context) {
	poll := PollInterval*rand.Intn(10) + PollInterval/2
	ticker := time.NewTicker(time.Duration(poll) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				s.fetchOnce(ctx)
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (s *ScheduleJob) fetchOnce(ctx context.Context) {
	jobs, err := s.cache.TimeQuery(ctx, s.queue.Name)
	if err != nil {
		log.Printf("Fetch error:%s\n", err)
	}
	log.Printf("Fetch queue:[%s] job:%s\n", s.queue.Name, jobs)
	for _, job := range jobs {
		//if not ticker
		_, err := s.cache.TimeRem(ctx, s.queue.Name, job)
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

func NewGoDot(ctx context.Context, client *redis.Client, queues []Queue, maxRedisCnn int) *GoDot {
	cache := NewRedisCache(client)
	jobs := make(chan string)

	retryJob := NewScheduleJob(ctx, RetryQueue, cache, jobs)
	scheduleJob := NewScheduleJob(ctx, ScheduleQueue, cache, jobs)

	const OtherCnn = 2
	queueJob := NewQueueJob(ctx, queues, cache, jobs, maxRedisCnn-OtherCnn)

	const DotsPerCnn = 50
	dots := NewDot(maxRedisCnn * DotsPerCnn)
	dots.WaitJob(ctx, jobs, retryJob)

	godot := GoDot{
		cache:       cache,
		queueJob:    queueJob,
		retryJob:    retryJob,
		scheduleJob: scheduleJob,
		dot:         dots,
	}

	//display info
	go func() {
		dt := time.NewTicker(time.Second * 10)
		for range dt.C {
			fmt.Printf("RedisCnn:%d Routines:%d\n", client.PoolStats().TotalConns, runtime.NumGoroutine())
		}
		defer dt.Stop()
	}()

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

func (d *Client) Run(ctx context.Context, className string, args ...interface{}) {
	runData, err := NewDotData(className)
	if err != nil {
		log.Println("Init run data error:", err)
	}
	runData.Args = args
	enqueue, err := jsoniter.Marshal(runData)
	if err != nil {
		log.Println("Enqueue json marshal retryJob:", err)
	}
	d.cache.Push(ctx, runData.Queue, string(enqueue))
}

func (d *Client) RunAt(ctx context.Context, at int64, className string, args ...interface{}) {
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
	d.cache.TimeAdd(ctx, at, runData.Queue, string(enqueue))
}
