package lib

import (
	"fmt"
	"log"
	"testing"

	"openpitrix.io/logger"
)

func TestConnect(t *testing.T) {
	endpoints := []string{"192.168.0.6:12379"}
	q := new(EtcdQueue)
	e, err := q.Connect(endpoints)
	log.Println(e)
	if err != nil {
		t.Fatal(err)
	}
}


func TestEnqueue(t *testing.T) {
	endpoints := []string{"192.168.0.6:12379"}
	q := new(EtcdQueue)
	client, err := q.Connect(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	q=q.NewQueue(client,"notification")

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("notification_%d", i)
		err := q.Enqueue(id)
		if err != nil {
			logger.Errorf(nil, "Failed to Enqueue notification from etcd queue: %+v", err)
		}
		logger.Infof(nil, "Enqueue notification [%s] from etcd queue succeed", id)
	}
}

func TestDequeue(t *testing.T) {
	endpoints := []string{"192.168.0.6:12379"}
	q := new(EtcdQueue)
	client, err := q.Connect(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	q=q.NewQueue(client,"notification")
	for i := 0; i < 100; i++ {
		n, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		logger.Infof(nil, "Got message [%s] from queue, worker number [%d]", n, i)
	}
}












