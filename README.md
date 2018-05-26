# Universe: DSM platform

Usage guide:

* Start monitor: ./monitor -n WORKER_NUM
* Start workers: ./worker -r RANK -s MON_IP:MON_PORT

Default port 10086

rank should be distinct and lower than WORKER_NUM

When all workers are ready, they will exit `prepare_worker()` function
and do stuff that you want.


