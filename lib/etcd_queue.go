package lib

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/contrib/recipes"
	"time"
)

type EtcdClient struct {
	*clientv3.Client
}

type EtcdQueue struct {
	*recipe.Queue
}

func (q *EtcdQueue)Connect(endpoints []string) (*EtcdClient, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	cli.KV = namespace.NewKV(cli.KV, "")
	cli.Watcher = namespace.NewWatcher(cli.Watcher, "")
	cli.Lease = namespace.NewLease(cli.Lease, "")
	return &EtcdClient{cli}, err
}


func (q *EtcdQueue) NewQueue(client *EtcdClient,topic string) *EtcdQueue {
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
