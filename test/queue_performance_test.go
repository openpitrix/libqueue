// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package test

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/coreos/etcd/version"

	i "openpitrix.io/libqueue"
	q "openpitrix.io/libqueue/queue"
)

func enqueue(queue i.IQueue) int {
	val := fmt.Sprintf("%d", rand.Intn(10000))
	err := queue.Enqueue(val)
	//logger.Infof(nil, "enqueue  queue.topic=%s", queue.topic)
	if err != nil {
		//logger.Infof(nil, "enqueue error=%+v", err)
		return 1
	}
	return 0
}

func dequeue(queue i.IQueue) int {
	_, err := queue.Dequeue()
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

func enqueueWorker(queue *i.IQueue) {
	timer := time.NewTicker(time.Millisecond * 1000)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			ch <- enqueue(*queue)
		}
	}
}

func dequeueWorker(queue *i.IQueue) {
	timer := time.NewTicker(time.Millisecond * 1000)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			ch <- dequeue(*queue)
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

	pubsubConnStr := "192.168.0.6:12379"
	pubsubType := "etcd"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr,
	}
	iClient, _ := q.NewIClient(pubsubType, pubsubConfigMap)
	iqueue, _ := q.NewIQueue(pubsubType, &iClient)

	for i := 0; i < TestingTasksCnt; i++ {
		topicName := "notification_" + strconv.Itoa(int(i))
		iqueue.SetTopic(topicName)
		go enqueueWorker(&iqueue)
	}

	go summarize()
	go calc()

	for {
		time.Sleep(time.Second * 3600)
	}
}

func TestDeQueuePerf4Etcd(t *testing.T) {
	fmt.Printf("ETCD Version %v\n", version.Version)

	pubsubConnStr := "192.168.0.6:12379"
	pubsubType := "etcd"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr,
	}
	iClient, _ := q.NewIClient(pubsubType, pubsubConfigMap)
	iqueue, _ := q.NewIQueue(pubsubType, &iClient)

	for i := 0; i < TestingTasksCnt; i++ {
		topicName := "notification_" + strconv.Itoa(int(i))
		iqueue.SetTopic(topicName)
		go dequeueWorker(&iqueue)
	}
	go summarize()
	go calc()

	for {
		time.Sleep(time.Second * 3600)
	}
}

func TestEnQueuePerf4Redis(t *testing.T) {
	pubsubConnStr := "redis://192.168.0.6:6379"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr}
	pubsubType := "redis"
	iClient, _ := q.NewIClient(pubsubType, pubsubConfigMap)
	iqueue, _ := q.NewIQueue(pubsubType, &iClient)

	for i := 0; i < TestingTasksCnt; i++ {
		topicName := "notification_" + strconv.Itoa(int(i))
		iqueue.SetTopic(topicName)
		go enqueueWorker(&iqueue)
	}

	go summarize()
	go calc()

	for {
		time.Sleep(time.Second * 3600)
	}
}

func TestDeQueuePerf4Redis(t *testing.T) {
	pubsubConnStr := "redis://192.168.0.6:6379"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr}
	pubsubType := "redis"
	iClient, _ := q.NewIClient(pubsubType, pubsubConfigMap)
	iqueue, _ := q.NewIQueue(pubsubType, &iClient)

	for i := 0; i < TestingTasksCnt; i++ {
		topicName := "notification_" + strconv.Itoa(int(i))
		iqueue.SetTopic(topicName)
		go dequeueWorker(&iqueue)
	}

	go summarize()
	go calc()

	for {
		time.Sleep(time.Second * 3600)
	}
}
