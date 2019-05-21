package lib

import (
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	recipe "github.com/coreos/etcd/contrib/recipes"
)

type EtcdClient struct {
	*clientv3.Client
}

type EtcdQueue struct {
	*recipe.Queue
}

func (client *EtcdClient) NewQueue(topic string) *EtcdQueue {
	return &EtcdQueue{recipe.NewQueue(client.Client, topic)}
}

func (q *EtcdQueue) Enqueue(val string) error {
	return q.Queue.Enqueue(val)
}

// Dequeue returns Enqueue()'d elements in FIFO order. If the
// queue is empty, Dequeue blocks until elements are available.
func (q *EtcdQueue) Dequeue() (string, error) {
	return q.Queue.Dequeue()
}

func Connect(endpoints []string, prefix string) (*EtcdClient, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	cli.KV = namespace.NewKV(cli.KV, prefix)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, prefix)
	cli.Lease = namespace.NewLease(cli.Lease, prefix)
	return &EtcdClient{cli}, err
}
