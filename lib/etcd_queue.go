package lib

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/contrib/recipes"

	"time"
)

type EtcdQueue struct {
	*clientv3.Client
	topic string
}

func (q *EtcdQueue) Connect(connStrs []string) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   connStrs,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	cli.KV = namespace.NewKV(cli.KV, "")
	cli.Watcher = namespace.NewWatcher(cli.Watcher, "")
	cli.Lease = namespace.NewLease(cli.Lease, "")
	return cli, nil
}

func (q *EtcdQueue) NewQueue(client *clientv3.Client, topic string) *EtcdQueue {
	return &EtcdQueue{client, topic}
}

func (q *EtcdQueue) Enqueue(cli *clientv3.Client, val string) error {
	etcdQueue := recipe.NewQueue(cli, q.topic)
	return etcdQueue.Enqueue(val)
}

// Dequeue returns Enqueue()'d elements in FIFO order. If the queue is empty, Dequeue blocks until elements are available.
func (q *EtcdQueue) Dequeue(cli *clientv3.Client) (string, error) {
	etcdQueue := recipe.NewQueue(cli, q.topic)
	return etcdQueue.Dequeue()
}
