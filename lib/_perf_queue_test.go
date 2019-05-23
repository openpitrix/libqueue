package lib

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func enqueue(queue *QCQueue, cli QCClient) int {
	err := queue.Enqueue(cli, fmt.Sprintf("%d", rand.Intn(10000)))
	//logger.Infof(nil, "enqueue  queue.topic=%s", queue.topic)
	if err != nil {
		return 1
	}
	return 0
}

func dequeue(queue *QCQueue, cli QCClient) int {
	_, err := queue.Dequeue(cli)
	//logger.Infof(nil, "enqueue  queue.topic=%s", queue.topic)
	if err != nil {
		return 3
	}
	//fmt.Printf("%+v", value)
	return 2
}

const (
	EnqueueTasks = 1000
	DequeueTasks = 1000
	Maxtasks     = 10000
)

var successEnqueue = 0
var errorEnqueue = 0
var successDequeue = 0
var errorDequeue = 0
var ch = make(chan int, (Maxtasks+2)*2)

func enqueueWorker(queue *QCQueue, cli QCClient) {
	for {
		ch <- enqueue(queue, cli)
	}
}

func dequeueWorker(queue *QCQueue, cli QCClient) {
	for {
		ch <- dequeue(queue, cli)
	}
}

func summarize() {
	for amount := range ch {
		switch amount {
		case 0:
			successEnqueue += 1
		case 1:
			errorEnqueue += 1
		case 2:
			successDequeue += 1
		case 3:
			errorDequeue += 1
		case -1:
			successEnqueue = 0
			errorEnqueue = 0
			successDequeue = 0
			errorDequeue = 0
		}
	}
}

func calc() {
	for {
		time.Sleep(time.Second * 1)
		fmt.Printf("Enqueue %d, %d, %d Dequeue %d, %d, %d\n", successEnqueue, errorEnqueue, successEnqueue+errorEnqueue, successDequeue, errorDequeue, successDequeue+errorDequeue)
		fmt.Printf("Go Routine Number %+v\n", runtime.NumGoroutine())
		ch <- -4
	}
}

func TestQueuePerf(t *testing.T) {
	connStrs := []string{"192.168.0.6:12379"}
	cli, _ := new(EtcdQueue).Connect(connStrs)

	for i := 0; i < EnqueueTasks; i++ {
		q := new(QCQueue)
		q.topic = "notification_" + strconv.Itoa(int(i))
		q.queueType = "etcd"
		q.client = cli
		go enqueueWorker(q, cli)
	}

	for i := 0; i < DequeueTasks; i++ {
		q := new(QCQueue)
		q.topic = "notification_" + strconv.Itoa(int(i))
		q.queueType = "etcd"
		q.client = cli
		go dequeueWorker(q, cli)
	}

	go summarize()
	go calc()

	for {
		time.Sleep(time.Second * 3600)
	}

}

func TestQueuePerf2(t *testing.T) {

	connStr := []string{"192.168.0.4:6379", "", "2000"}
	cli, _ := new(RedisQueue).Connect(connStr)

	for i := 0; i < EnqueueTasks; i++ {
		q := new(QCQueue)
		q.topic = "notification_" + strconv.Itoa(int(i))
		q.queueType = "redis"
		q.client = cli

		go enqueueWorker(q, cli)
	}

	for i := 0; i < DequeueTasks; i++ {
		q := new(QCQueue)
		q.topic = "notification_" + strconv.Itoa(int(i))
		q.queueType = "redis"
		q.client = cli
		go dequeueWorker(q, cli)
	}

	go summarize()
	go calc()

	for {
		time.Sleep(time.Second * 3600)
	}

}
