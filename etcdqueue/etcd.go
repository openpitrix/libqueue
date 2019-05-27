package etcdqueue

import (
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	recipe "github.com/coreos/etcd/contrib/recipes"

	lib "openpitrix.io/libqueue"
)

type client struct {
	*clientv3.Client
}

func New(connStr string) (lib.QueueClient, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(connStr, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	cli.KV = namespace.NewKV(cli.KV, "")
	cli.Watcher = namespace.NewWatcher(cli.Watcher, "")
	cli.Lease = namespace.NewLease(cli.Lease, "")

	return client{cli}, err
}

func (c client) GetTopic(t string) (lib.Topic, error) {
	etcdTopic := topic{c.Client, t}
	return etcdTopic, nil
}

type topic struct {
	*clientv3.Client
	t string
}

func (t topic) Enqueue(val string) error {
	etcdQueue := recipe.NewQueue(t.Client, t.t)
	return etcdQueue.Enqueue(val)
}

func (t topic) Dequeue() (string, error) {
	etcdQueue := recipe.NewQueue(t.Client, t.t)
	return etcdQueue.Dequeue()
}

func (t topic) Topic() string {
	return t.t
}
