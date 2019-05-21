package lib

import (
	"fmt"
)

// Service interface describes all functions that must be implemented.
type QCQueue interface {
	NewQueue(topic string) *QCQueue
	Enqueue(val string) error
	Dequeue() (string, error)
	Connect(endpoints []string, prefix string) (*QCClient, error)
}

type QCClient interface {
}

func GetQCClient(queueType string) (QCClient, error) {
	switch queueType {
	case "etcd":
		return new(*EtcdClient), nil
	default:
		return nil, fmt.Errorf("unsupported queueType [%s]", queueType)
	}
}
