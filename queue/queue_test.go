package queue

import (
	"fmt"
	"testing"

	"openpitrix.io/logger"
)

func TestQueue4Etcd(t *testing.T) {
	connStr := "192.168.0.6:12379"
	var configMap map[string]interface{}
	configMap = make(map[string]interface{})
	configMap["connStr"] = connStr
	cli, _ := New("etcd", configMap)
	topic, _ := cli.SetTopic("test")

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
	connStr := "redis://192.168.0.4:6379"
	var configMap map[string]interface{}
	configMap = make(map[string]interface{})
	configMap["connStr"] = connStr
	configMap["PoolSize"] = 2000
	configMap["MinIdleConns"] = 1
	cli, err := New("redis", configMap)
	if err != nil {
		logger.Errorf(nil, "err:=[%+v]", err)
	}
	topic, err := cli.SetTopic("test")
	if err != nil {
		logger.Errorf(nil, "err:=[%+v]", err)
	}
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
