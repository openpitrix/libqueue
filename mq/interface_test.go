package mq

import (
	"fmt"
	"testing"

	"openpitrix.io/logger"
)

func TestInterface4Etcd(t *testing.T) {
	connStrs := []string{"192.168.0.6:12379"}
	etcd := new(Etcd)

	c, _ := etcd.Connect(connStrs)
	topic, _ := c.(*Etcd).GetTopic("test")

	topic.Enqueue("sss")
	topic.Dequeue()

	etcdTopic := topic.(*EtcdTopic)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("notification_%d", i)
		err := topic.Enqueue(id)
		if err != nil {
			logger.Errorf(nil, "Failed to Enqueue notification from etcd queue: %+v", err)
		} else {
			logger.Infof(nil, "Enqueue notification [%s] into etcd queue succeed", id)
		}
	}

	for i := 0; i < 10; i++ {
		msg, err := topic.Dequeue()
		if err != nil {
			t.Fatal(err)
		} else {
			logger.Infof(nil, "Got message [%s] from etcd queue[%s] . ", msg, etcdTopic.topic)
		}
	}

}

func TestInterface4Redis(t *testing.T) {
	connStr := []string{"192.168.0.4:6379", "", "2000"}
	q := new(Redis)
	c, _ := q.Connect(connStr)
	topic, _ := c.(*Redis).GetTopic("test")
	topic.Enqueue("sss")
	topic.Dequeue()

	redisTopic := topic.(*RedisTopic)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("notification_%d", i)
		err := topic.Enqueue(id)
		if err != nil {
			logger.Errorf(nil, "Failed to Enqueue notification from etcd queue: %+v", err)
		} else {
			logger.Infof(nil, "Enqueue notification [%s] into etcd queue succeed", id)
		}
	}

	for i := 0; i < 10; i++ {
		msg, err := topic.Dequeue()
		if err != nil {
			t.Fatal(err)
		} else {
			logger.Infof(nil, "Got message [%s] from etcd queue[%s] . ", msg, redisTopic.topic)
		}
	}

}
