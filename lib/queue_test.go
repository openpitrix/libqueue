package lib

import (
	"fmt"
	"openpitrix.io/logger"
	"testing"
)

func TestConnect4Queue(t *testing.T) {
	etcdConnStr := []string{"192.168.0.6:12379"}
	q := new(QCQueue)
	q.topic = "notification"
	q.connStrs = etcdConnStr
	q.queueType = "etcd"
	q.Connect()

}

func TestEnqueue4EtcdQueue(t *testing.T) {
	etcdConnStr := []string{"192.168.0.6:12379"}
	q := new(QCQueue)
	q.topic = "notification"
	q.connStrs = etcdConnStr
	q.queueType = "etcd"
	cli, _ := q.Connect()

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("notification_%d", i)
		err := q.Enqueue(cli, id)
		if err != nil {
			logger.Errorf(nil, "Failed to Enqueue notification from etcd queue: %+v", err)
		} else {
			logger.Infof(nil, "Enqueue notification [%s] into etcd queue succeed", id)
		}
	}
}

func TestDequeue4EtcdQueue(t *testing.T) {
	etcdConnStr := []string{"192.168.0.6:12379"}
	q := new(QCQueue)
	q.topic = "notification"
	q.connStrs = etcdConnStr
	q.queueType = "etcd"

	cli, _ := q.Connect()
	for i := 0; i < 100; i++ {
		msg, err := q.Dequeue(cli)
		if err != nil {
			t.Fatal(err)
		} else {
			logger.Infof(nil, "Got message [%s] from etcd queue[%s]. ", msg, q.topic)
		}
	}

}

func TestEnqueue4RedisQueue(t *testing.T) {
	connStr := []string{"192.168.0.4:6379", "", "2000"}
	q := new(QCQueue)
	q.topic = "notification"
	q.connStrs = connStr
	q.queueType = "redis"

	cli, _ := q.Connect()

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("notification_%d", i)
		err := q.Enqueue(cli, id)
		if err != nil {
			logger.Errorf(nil, "Failed to Enqueue notification from redis queue: %+v", err)
		} else {
			logger.Infof(nil, "Enqueue notification [%s] into redis queue succeed", id)
		}
	}
}

func TestDequeue4RedisQueue(t *testing.T) {
	connStr := []string{"192.168.0.4:6379", "", "2000"}
	q := new(QCQueue)
	q.topic = "notification"
	q.connStrs = connStr
	q.queueType = "redis"

	cli, _ := q.Connect()

	for i := 0; i < 100; i++ {
		msg, err := q.Dequeue(cli)
		if err != nil {
			t.Fatal(err)
		} else {
			logger.Infof(nil, "Got message [%s] from redis queue[%s]. ", msg, q.topic)
		}
	}
}
