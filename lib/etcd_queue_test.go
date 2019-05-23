package lib

import (
	"fmt"
	"testing"

	"openpitrix.io/logger"
)

func TestConnect(t *testing.T) {
	connStrs := []string{"192.168.0.6:12379"}
	q := new(EtcdQueue)
	_, err := q.Connect(connStrs)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnqueue(t *testing.T) {
	connStrs := []string{"192.168.0.6:12379"}
	q := new(EtcdQueue)
	cli, err := q.Connect(connStrs)
	if err != nil {
		t.Fatal(err)
	}

	etcdq := q.NewQueue(cli, "sss")

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("notification_%d", i)
		err := etcdq.Enqueue(cli, id)
		if err != nil {
			logger.Errorf(nil, "Failed to Enqueue notification from etcd queue: %+v", err)
		}
		logger.Infof(nil, "Enqueue notification [%s] from etcd queue succeed", id)
	}
}

func TestDequeue(t *testing.T) {
	connStrs := []string{"192.168.0.6:12379"}

	q := new(EtcdQueue)
	cli, err := q.Connect(connStrs)
	if err != nil {
		t.Fatal(err)
	}

	etcdq := q.NewQueue(cli, "sss")
	for i := 0; i < 100; i++ {
		n, err := etcdq.Dequeue(cli)
		if err != nil {
			t.Fatal(err)
		}
		logger.Infof(nil, "Got message [%s] from queue, worker number [%d]", n, i)
	}
}
