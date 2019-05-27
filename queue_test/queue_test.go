package queue_test

import (
	"fmt"
	"testing"

	"openpitrix.io/logger"

	e "openpitrix.io/libqueue/etcdqueue"
	r "openpitrix.io/libqueue/redisqueue"
)

func TestQueue4Etcd(t *testing.T) {
	connStrs := "192.168.0.6:12379"
	Cli, _ := e.New(connStrs)
	topic, _ := Cli.GetTopic("test")

	topic.Enqueue("sss")
	topic.Dequeue()

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("notification_%d", i)
		err := topic.Enqueue(id)
		if err != nil {
			logger.Errorf(nil, "Failed to Enqueue notification from etcd queue: %+v.", err)
		} else {
			logger.Infof(nil, "Enqueue notification [%s] into etcd queue succeed.", id)
		}
	}

	for i := 0; i < 10; i++ {
		msg, err := topic.Dequeue()
		if err != nil {
			t.Fatal(err)
		} else {
			logger.Infof(nil, "Got message [%s] from etcd queue. ", msg)
		}
	}

}

func TestInterface4Redis(t *testing.T) {
	connStrs := "redis://192.168.0.4:6379"
	Cli, _ := r.New(connStrs)
	topic, _ := Cli.GetTopic("test")

	topic.Enqueue("sss")
	topic.Dequeue()

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("notification_%d", i)
		err := topic.Enqueue(id)
		if err != nil {
			logger.Errorf(nil, "Failed to Enqueue notification from redis queue: %+v.", err)
		} else {
			logger.Infof(nil, "Enqueue notification [%s] into redis redis succeed.", id)
		}
	}

	for i := 0; i < 10; i++ {
		msg, err := topic.Dequeue()
		if err != nil {
			t.Fatal(err)
		} else {
			logger.Infof(nil, "Got message [%s] from redis queue.", msg)
		}
	}

}
