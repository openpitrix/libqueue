package lib

import (
	"fmt"
	"log"
	"openpitrix.io/logger"
	"testing"
)

func TestConnect4Redis(t *testing.T) {
	connStr := []string{"192.168.0.4:6379","","2000"}
	q := new(RedisQueue)
	client, err := q.Connect(connStr)
	log.Println(client)
	if err != nil {
		t.Fatal(err)
	}
}


func TestEnqueue4Redis(t *testing.T) {
	connStr := []string{"192.168.0.4:6379","","2000"}
	q := new(RedisQueue)
	client, err := q.Connect(connStr)
	if err != nil {
		t.Fatal(err)
	}
	q=q.NewQueue(client,"notification")

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("notification_%d", i)
		err := q.Enqueue(id)
		if err != nil {
			logger.Errorf(nil, "Failed to Enqueue notification from redis queue: %+v", err)
		}
		logger.Infof(nil, "Enqueue notification [%s] from redis queue succeed", id)
	}
}

func TestDequeue4Redis(t *testing.T) {
	connStr := []string{"192.168.0.4:6379","","2000"}
	q := new(RedisQueue)
	client, err := q.Connect(connStr)
	if err != nil {
		t.Fatal(err)
	}
	q=q.NewQueue(client,"notification")
	for i := 0; i < 100; i++ {
		n, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		logger.Infof(nil, "Got message [%s] from redis queue, worker number [%d]", n, i)
	}
}


