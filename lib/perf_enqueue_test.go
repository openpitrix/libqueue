// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package lib

import (
	"fmt"
	"github.com/coreos/etcd/version"
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
		//logger.Infof(nil, "enqueue error=%+v", err)
		return 1
	}
	return 0
}

func dequeue(queue *QCQueue, cli QCClient) int {
	_, err := queue.Dequeue(cli)
	//logger.Infof(nil, "enqueue  queue.topic=%s", queue.topic)
	if err != nil {
		return 1
	}
	//fmt.Printf("%+v", value)
	return 0
}

const (
	TestingTasksCnt = 100
)

var successTaskCnt = 0
var errorTaskCnt = 0
var ch = make(chan int, (TestingTasksCnt+2)*2)

func enqueueWorker(queue *QCQueue, cli QCClient) {
	timer := time.NewTicker(time.Millisecond * 1000)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			ch <- enqueue(queue, cli)
		}
	}
}

func dequeueWorker(queue *QCQueue, cli QCClient) {
	timer := time.NewTicker(time.Millisecond * 1000)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			ch <- dequeue(queue, cli)
		}
	}
}

func summarize() {
	for amount := range ch {
		switch amount {
		case 0:
			successTaskCnt += 1
		case 1:
			errorTaskCnt += 1
		case -1:
			successTaskCnt = 0
			errorTaskCnt = 0
		}
	}
}

func calc() {
	for {
		time.Sleep(time.Second * 1)
		fmt.Printf("Task result successTask=%d, errorTask=%d, totalTask=%d    ", successTaskCnt, errorTaskCnt, successTaskCnt+errorTaskCnt)
		fmt.Printf("Go Routine Number %+v\n", runtime.NumGoroutine())
		ch <- -1
	}
}

func TestEnQueuePerf4Etcd(t *testing.T) {
	fmt.Printf("ETCD Version %v\n", version.Version)

	connStrs := []string{"192.168.0.6:12379"}
	cli, _ := new(EtcdQueue).Connect(connStrs)

	for i := 0; i < TestingTasksCnt; i++ {
		q := new(QCQueue)
		q.topic = "notification_" + strconv.Itoa(int(i))
		q.queueType = "etcd"
		q.client = cli
		go enqueueWorker(q, cli)
	}

	go summarize()
	go calc()

	for {
		time.Sleep(time.Second * 3600)
	}
}

func TestDeQueuePerf4Etcd(t *testing.T) {
	fmt.Printf("ETCD Version %v\n", version.Version)

	connStrs := []string{"192.168.0.6:12379"}
	cli, _ := new(EtcdQueue).Connect(connStrs)

	for i := 0; i < TestingTasksCnt; i++ {
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

func TestEnQueuePerf4Redis(t *testing.T) {

	connStr := []string{"192.168.0.4:6379", "", "2000"}
	cli, _ := new(RedisQueue).Connect(connStr)

	for i := 0; i < TestingTasksCnt; i++ {
		q := new(QCQueue)
		q.topic = "notification_" + strconv.Itoa(int(i))
		q.queueType = "redis"
		q.client = cli
		go enqueueWorker(q, cli)
	}

	go summarize()
	go calc()

	for {
		time.Sleep(time.Second * 3600)
	}
}

func TestDeQueuePerf4Redis(t *testing.T) {

	connStr := []string{"192.168.0.4:6379", "", "2000"}
	cli, _ := new(RedisQueue).Connect(connStr)

	for i := 0; i < TestingTasksCnt; i++ {
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
