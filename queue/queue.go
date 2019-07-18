// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/go-redis/redis"
	"openpitrix.io/logger"

	i "openpitrix.io/libqueue"
	qetcd "openpitrix.io/libqueue/etcd"
	qredis "openpitrix.io/libqueue/redis"
)

func NewIQueue(queueType string, qclient *i.IClient) (i.IQueue, error) {
	var iqueue i.IQueue
	switch queueType {
	case "etcd":
		etcdQueue := qetcd.EtcdQueue{}
		iqueue = &etcdQueue
		iqueue.SetClient(qclient)
		return iqueue, nil
	case "redis":
		redisQueue := qredis.RedisQueue{}
		iqueue = &redisQueue
		iqueue.SetClient(qclient)
		return iqueue, nil
	default:
		return nil, fmt.Errorf("unsupported Queue Type [%s]", queueType)
	}
}

func NewIPubSub(pubsubType string, qclient *i.IClient) (i.IPubSub, error) {
	var ipubsub i.IPubSub
	switch pubsubType {
	case "etcd":
		etcdPubSub := qetcd.EtcdPubSub{}
		ipubsub = &etcdPubSub
		ipubsub.SetClient(qclient)
		return ipubsub, nil
	case "redis":
		redisPubSub := qredis.RedisPubSub{}
		ipubsub = &redisPubSub
		ipubsub.SetClient(qclient)
		return ipubsub, nil
	default:
		return nil, fmt.Errorf("unsupported Queue Type [%s]", pubsubType)
	}
}

func NewIClient(pubsubType string, configMap map[string]interface{}) (i.IClient, error) {
	if configMap == nil {
		return nil, fmt.Errorf("not provide queue configuration info.")
	}

	switch pubsubType {
	case "etcd":
		cfg := LoadConf4Etcd(configMap)
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
			logger.Errorf(nil, "new etcd queue failed, err=%+v", err)
			return nil, err
		}
		cli.KV = namespace.NewKV(cli.KV, "")
		cli.Watcher = namespace.NewWatcher(cli.Watcher, "")
		cli.Lease = namespace.NewLease(cli.Lease, "")
		return qetcd.EtcdClient{*cli}, err

	case "redis":
		cfg := loadConf4Redis(configMap)

		if cfg.ConnStr == "" {
			return nil, errors.New("not provide ConnStr parameter.")
		}

		options, err := redis.ParseURL(cfg.ConnStr)
		if err != nil {
			return nil, err
		}

		if cfg.PoolSize != 0 {
			options.PoolSize = cfg.PoolSize
		}

		if cfg.MinIdleConns != 0 {
			options.MinIdleConns = cfg.MinIdleConns
		}

		cli := redis.NewClient(options)
		_, err = cli.Ping().Result()
		if err != nil {
			logger.Debugf(nil, "err=%+v", err)
			return nil, err
		}

		return qredis.RedisClient{*cli}, nil
	default:
		return nil, fmt.Errorf("unsupported Queue Type [%s]", pubsubType)
	}
}

type redisConfig struct {
	ConnStr      string
	PoolSize     int
	MinIdleConns int
}

func loadConf4Redis(configMap map[string]interface{}) *redisConfig {
	mjson, _ := json.Marshal(configMap)
	mString := string(mjson)
	logger.Debugf(nil, "mString:%s", mString)

	var config redisConfig
	data := []byte(mString)
	err := json.Unmarshal(data, &config)
	if err != nil {
		fmt.Println(err)
	}
	return &config
}

type etcdConfig struct {
	ConnStr           string
	DialTimeoutSecond int
}

func LoadConf4Etcd(configMap map[string]interface{}) *etcdConfig {
	mjson, _ := json.Marshal(configMap)
	mString := string(mjson)

	var config etcdConfig
	data := []byte(mString)
	err := json.Unmarshal(data, &config)
	if err != nil {
		fmt.Println(err)
	}
	return &config
}
