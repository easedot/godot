Godot
==============
Simple, efficient background processing for Go.

    Godot uses go routine to handle many jobs at the same time in the
    same process
![Web UI](https://github.com/easedot/godot/blob/master/assets/godot.png)

Requirements
-----------------

    redis
    go 1.19+

Installation
-----------------

    go get github.com/easedot/godot

Getting Started
-----------------

Step1

    Write doters , doters/doters.go

Step2

    Write job servers, examples/examples_srv.go

Step3

    Write run job task, examples/examples_cli.go

Step4

    go build example/example_srv.go

    go build example/example_cli.go

Step5

    docker-compose up -d
    
    open new termial run     
    ./example_srv

    open new termial run
    ./example_cli

Config
---------------
    //set queue weight
	var queues = []godot.Queue{
		{Name: "work1", Weight: 3},
		{Name: "work2", Weight: 2},
		{Name: "work3", Weight: 1},
		{Name: "default", Weight: 1},
	}

    //set dots to 1000 goroutine 
    godotSRV := godot.NewGoDot(ctx, client, queues, 50, 6698)
    
    //set job option and register
	options := Doter{
		Queue:      "default",
		Retry:      false,
		RetryCount: 2,
	}
	doter := defaultDoter{options}
	Register(doter, options)

    client.Run(ctx, "defaultDoter", "test_at")
    //or run after 1000ms
    client.RunAt(ctx, 1000, "defaultDoter", "test_at")

Todo
---------------
    dashboard localhost:6698
![Web UI](https://github.com/easedot/godot/blob/master/assets/dashboard.jpg)

Performance
---------------
    Macbook M1 Pro  
    10 process 1m27s put 500000 jobs 
    ./example_cli -m 50000

    one server, 2000 redis conn, 30s done.
    ./example_srv -m 2000

License
-----------------

Please see [LICENSE.txt](https://github.com/easedot/godot/blob/master/LICENSE) for licensing details.

Author
-----------------

Haihui Jiang [@easedot](https://twitter.com/easedot)
