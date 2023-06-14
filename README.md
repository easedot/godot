Godot
==============
Simple, efficient background processing for Go.

Godot uses go routine to handle many jobs at the same time in the
same process

![Web UI](https://github.com/easedot/godot/blob/master/godot.png)

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

Step4

    docker-compose up -d
    
    open new termial run     
    ./example_srv

    open new termial run
    ./example_cli
     

Performance
---------------

License
-----------------

Please see [LICENSE.txt](https://github.com/easedot/godot/blob/master/LICENSE) for licensing details.

Author
-----------------

Haihui Jiang [@easedot](https//twitter.com/easedot)
