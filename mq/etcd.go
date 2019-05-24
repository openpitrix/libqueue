package mq

import (
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/contrib/recipes"
	"openpitrix.io/logger"
)

type Etcd struct {
	*clientv3.Client
}

func (c Etcd) Connect(connStrs []string) (QC, error) {
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
	logger.Infof(nil, "Connect done.")
	c.Client = cli
	return &Etcd{cli}, err
}

func (c Etcd) GetTopic(topic string) (Topic, error) {
	return &EtcdTopic{c.Client, topic}, nil
}

type EtcdTopic struct {
	*clientv3.Client
	topic string
}

func (t EtcdTopic) Enqueue(val string) error {
	etcdQueue := recipe.NewQueue(t.Client, t.topic)
	return etcdQueue.Enqueue(val)
}

func (t EtcdTopic) Dequeue() (string, error) {
	etcdQueue := recipe.NewQueue(t.Client, t.topic)
	return etcdQueue.Dequeue()
}
