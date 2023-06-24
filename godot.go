package godot

import (
	"context"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"html/template"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const MaxSecondSpan = 60 * 60 * 24 //seconds per 7 days
const MaxMinuteSpan = 60 * 24 * 7  //seconds per 7 days

const ScheduleQueue = "schedule"
const RetryQueue = "retry"
const STASQKEY = "godot:stats:%s"
const MSTASQKEY = "godot:mstats:%s"
const CALCKEY = "godot:calc:%s"
const Jobs = "jobs"
const Success = "success"
const Dead = "dead"

const (
	DefaultMaxRetry = 25
	LAYOUT          = "2006-01-02 15:04:05 UTC"
	FetchTimeout    = 2 * time.Second
	PollInterval    = 2
)

var printer = message.NewPrinter(language.English)

type Dot struct {
	stats   *Stats
	retry   *ScheduleJob
	jobs    chan string
	wg      sync.WaitGroup
	maxDots int
}

func NewDot(maxDots int, stats *Stats) *Dot {
	dot := Dot{maxDots: maxDots, stats: stats}
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
				//log.Printf("DOT[%d] Process job...\n", i)
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
	d.stats.PushHash(ctx, Jobs, Success)
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
		d.stats.PushHash(ctx, Jobs, Dead)
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
func (q *QueueJob) QueueNames() []string {
	var qs []string
	for _, q := range q.queues {
		qs = append(qs, q.Name)
	}
	return qs
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
				job, err := q.cache.BlockRPop(ctx, queue...)
				if err == redis.Nil {
					//log.Printf("Fetch queue blank:%s job:[]\n", queue)
				} else if err != nil {
					log.Printf("Fetch queue:%s error:[%s]\n ", queue, err)
					q.idl <- true
				} else {
					log.Printf("Fetch queue:%s job:[%s]\n", queue, job)
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
	//log.Printf("Fetch queue:[%s] job:%s\n", s.queue.Name, jobs)
	//go spinner(100 * time.Millisecond)
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

type Stats struct {
	queueNames []string
	port       int
	cache      DotCache
}

func NewStats(cache DotCache, queues []Queue, port int) *Stats {
	queueNames := []string{ScheduleQueue, RetryQueue}
	for _, q := range queues {
		queueNames = append(queueNames, q.Name)
	}

	stats := Stats{
		queueNames: queueNames,
		cache:      cache,
		port:       port,
	}

	go stats.pushStatus()
	go stats.calcStatus()
	go stats.waitServer()
	return &stats
}

type ChartData struct {
	Name   string
	Title  string            `json:"title,omitempty"`
	Legend []string          `json:"legend,omitempty"`
	Labels []string          `json:"labels,omitempty"`
	Series []ChartSeriesData `json:"series,omitempty"`
	Height string
}
type ChartSeriesData struct {
	Name      string `json:"name,omitempty"`
	Type      string `json:"type,omitempty"`
	Data      []int  `json:"data,omitempty"`
	Smooth    bool   `json:"smooth,omitempty"`
	Animation bool   `json:"animation,omitempty"`
}

func (d *Stats) pushStatus() {
	ctx := context.Background()
	sdt := time.NewTicker(time.Second)
	defer sdt.Stop()

	for range sdt.C {
		for _, q := range d.queueNames {
			go d.pushData(ctx, q)
		}
	}
}

func (d *Stats) pushData(ctx context.Context, q string) {
	statsKey := fmt.Sprintf(STASQKEY, q)
	if c, err := d.cache.LLen(ctx, q); err == nil {
		d.cache.RPush(ctx, statsKey, c)
	} else {
		d.cache.RPush(ctx, statsKey, 0)
	}
	if l, err := d.cache.LLen(ctx, statsKey); err == nil && l > MaxSecondSpan {
		if _, err = d.cache.LPop(ctx, statsKey); err != nil {
			log.Printf("LPop error %s key %s", err, statsKey)
		}
	}
}

func (d *Stats) calcStatus() {
	ctx := context.Background()
	mdt := time.NewTicker(time.Minute)
	defer mdt.Stop()
	for range mdt.C {
		for _, q := range d.queueNames {
			go d.calcData(ctx, q)
		}
	}
}

func (d *Stats) calcData(ctx context.Context, q string) {
	statsKey := fmt.Sprintf(STASQKEY, q)
	mstatsKey := fmt.Sprintf(MSTASQKEY, q)
	if lv, err := d.cache.LRange(ctx, statsKey, -60, -1); err == nil {
		av := max(lv)
		d.cache.RPush(ctx, mstatsKey, av)
	} else {
		d.cache.RPush(ctx, mstatsKey, 0)
	}
	if l, err := d.cache.LLen(ctx, mstatsKey); err == nil && l > MaxMinuteSpan {
		d.cache.LPop(ctx, mstatsKey)
	}
}

func (d *Stats) waitServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", d.jobStats)
	host := fmt.Sprintf("localhost:%d", d.port)
	log.Printf("Server stats %s", host)
	log.Fatal(http.ListenAndServe(host, mux))
}

func (d *Stats) jobStats(w http.ResponseWriter, req *http.Request) {
	//defer trace("jobstats")()
	ctx := context.Background()
	minuteCh := make(chan *ChartData)
	hourCh := make(chan *ChartData)
	dayCh := make(chan *ChartData)
	go func() {
		minute := d.chartData(ctx, "minute", "Minute", 60, STASQKEY, "s", "240px")
		minuteCh <- minute
	}()
	go func() {
		hour := d.chartData(ctx, "hour", "Hour", 60, MSTASQKEY, "m", "240px")
		hourCh <- hour
	}()
	go func() {
		day := d.chartData(ctx, "days", "Day", 24*60, MSTASQKEY, "m", "240px")
		dayCh <- day
	}()

	cd := []*ChartData{<-minuteCh, <-hourCh, <-dayCh}

	success := d.GetHash(ctx, Jobs, Success)
	dead := d.GetHash(ctx, Jobs, Dead)
	succ, _ := strconv.Atoi(success)
	dd, _ := strconv.Atoi(dead)
	cds := struct {
		Charts     []*ChartData
		Goroutines int
		Success    string
		Dead       string
	}{
		cd,
		runtime.NumGoroutine(),
		printer.Sprintf("%d\n", succ),
		printer.Sprintf("%d\n", dd),
	}
	t, _ := template.ParseFiles("dashboard.html")
	if err := t.Execute(w, cds); err != nil {
		log.Printf("Execute html error %s", err)
	}
}

func (d *Stats) PushHash(ctx context.Context, hash, key string) {
	calcKey := fmt.Sprintf(STASQKEY, hash)
	if err := d.cache.HIncrBy(ctx, calcKey, key, 1); err != nil {
		log.Printf("push calc hash[%s] key [%s] err:%s", calcKey, key, err)
	}
}

func (d *Stats) GetHash(ctx context.Context, hash, key string) string {
	calcKey := fmt.Sprintf(STASQKEY, hash)
	if h, err := d.cache.HGet(ctx, calcKey, key); err != nil {
		//log.Printf("get calc hash[%s] key [%s]  err:%s", calcKey, key, err)
		return ""
	} else {
		return h
	}
}

func (d *Stats) chartData(ctx context.Context, name, title string, span int, keyMode, end string, height string) *ChartData {
	//defer trace(title)()
	start := -int64(span)
	const stop = -1
	var labels = make([]string, span)
	for i := 0; i < span; i++ {
		labels[i] = fmt.Sprintf("%d%s", i, end)
	}

	queues := d.queueNames
	var legend = make([]string, len(queues))
	ch := make(chan map[string][]int, len(queues))
	wg := sync.WaitGroup{}
	wg.Add(len(queues))
	for i := 0; i < len(queues); i++ {
		q := queues[i]
		legend[i] = q
		go func(q string) {
			defer wg.Done()
			qkey := fmt.Sprintf(keyMode, q)
			if lv, err := d.cache.LRange(ctx, qkey, start, stop); err == nil {
				var td = make([]int, span)
				lvl := len(lv)
				for k := 0; k < lvl; k++ {
					if v, err := strconv.Atoi(lv[k]); err != nil {
						log.Printf("Atoi error %s", err)
					} else {
						is := span - lvl
						td[is+k] = v
					}
				}
				var tdc = make(map[string][]int)
				tdc[q] = td
				ch <- tdc
			} else {
				log.Printf("lrange err %s %s %d %d", err, qkey, start, stop)
			}
		}(q)
	}
	wg.Wait()
	close(ch)
	var data []ChartSeriesData
	for lv := range ch {
		for k, v := range lv {
			data = append(data, ChartSeriesData{Name: k, Data: v, Type: "line", Smooth: true, Animation: false})
		}
	}

	cd := ChartData{
		Height: height,
		Name:   name,
		Title:  title,
		Labels: labels,
		Legend: legend,
		Series: data,
	}
	return &cd
}

type GoDot struct {
	cache       DotCache
	stats       *Stats
	retryJob    *ScheduleJob
	scheduleJob *ScheduleJob
	queueJob    *QueueJob
	dot         *Dot
	port        int
}

func NewGoDot(ctx context.Context, client *redis.Client, queues []Queue, maxRedisCnn int, port int) *GoDot {
	cache := NewRedisCache(client)
	jobs := make(chan string)

	retryJob := NewScheduleJob(ctx, RetryQueue, cache, jobs)
	scheduleJob := NewScheduleJob(ctx, ScheduleQueue, cache, jobs)

	queueJob := NewQueueJob(ctx, queues, cache, jobs, maxRedisCnn)

	stats := NewStats(cache, queues, port)

	const DotsPerCnn = 50
	dots := NewDot(maxRedisCnn*DotsPerCnn, stats)
	dots.WaitJob(ctx, jobs, retryJob)

	godot := GoDot{
		stats:       stats,
		cache:       cache,
		queueJob:    queueJob,
		retryJob:    retryJob,
		scheduleJob: scheduleJob,
		dot:         dots,
		port:        port,
	}

	return &godot
}

func (d *GoDot) WaitIdl() {
	for !d.queueJob.IsIDL() {
		//log.Println("Wait idl...")
	}
}

func (d *GoDot) WaitJob() {
	go spinner(100 * time.Millisecond)
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
	d.cache.LPush(ctx, runData.Queue, string(enqueue))
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
