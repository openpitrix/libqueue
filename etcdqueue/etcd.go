package etcdqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/contrib/recipes"
	"openpitrix.io/logger"

	lib "openpitrix.io/libqueue"
)

type client struct {
	*clientv3.Client
}

func (c client) SetTopic(t string) (lib.Topic, error) {
	etcdTopic := Topic{c.Client, t}
	return etcdTopic, nil
}

type Topic struct {
	*clientv3.Client
	t string
}

func (t Topic) New(configMap map[string]interface{}) (lib.QueueClient, error) {
	cfg := LoadConf(configMap)

	if cfg.ConnStr == "" {
		return nil, errors.New("not provide ConnStr parameter.")
	}

	var dialTimeout time.Duration = (time.Duration(5) * 1000) * time.Millisecond
	if cfg.DialTimeoutSecond != 0 {
		dialTimeout = (time.Duration(cfg.DialTimeoutSecond) * 1000) * time.Millisecond
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(cfg.ConnStr, ","),
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, err
	}
	cli.KV = namespace.NewKV(cli.KV, "")
	cli.Watcher = namespace.NewWatcher(cli.Watcher, "")
	cli.Lease = namespace.NewLease(cli.Lease, "")

	return client{cli}, err
}

func (t Topic) Enqueue(val string) error {
	etcdQueue := recipe.NewQueue(t.Client, t.t)
	return etcdQueue.Enqueue(val)
}

func (t Topic) Dequeue() (string, error) {
	etcdQueue := recipe.NewQueue(t.Client, t.t)
	return etcdQueue.Dequeue()
}

func (t Topic) Topic() string {
	return t.t
}

type EtcdConfig struct {
	ConnStr           string
	DialTimeoutSecond int
}

func LoadConf(configMap map[string]interface{}) *EtcdConfig {
	mjson, _ := json.Marshal(configMap)
	mString := string(mjson)
	logger.Infof(nil, "mString:%s", mString)

	var config EtcdConfig
	data := []byte(mString)
	err := json.Unmarshal(data, &config)
	if err != nil {
		fmt.Println(err)
	}
	return &config
}
