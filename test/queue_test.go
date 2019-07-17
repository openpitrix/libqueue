// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package test

import (
	"testing"

	"openpitrix.io/logger"

	i "openpitrix.io/libqueue"
	c "openpitrix.io/libqueue/client"
	etcdq "openpitrix.io/libqueue/etcd"
	redisq "openpitrix.io/libqueue/redis"
)

func TestEnqueue4Redis(t *testing.T) {
	pubsubConnStr := "redis://192.168.0.6:6379"
	pubsubType := "redis"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr}
	iClient, _ := c.NewIClient(pubsubType, pubsubConfigMap)

	redisQueue := redisq.RedisQueue{}
	var iqueue i.IQueue
	iqueue = &redisQueue
	iqueue.SetClient(&iClient)
	iqueue.SetTopic("test_topic1")
	iqueue.Enqueue("ssss")
}

func TestDequeue4Redis(t *testing.T) {
	pubsubConnStr := "redis://192.168.0.6:6379"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr}

	iClient, _ := c.NewIClient("redis", pubsubConfigMap)

	redisQueue := redisq.RedisQueue{}
	var iqueue i.IQueue
	iqueue = &redisQueue
	iqueue.SetClient(&iClient)
	iqueue.SetTopic("test_topic1")
	val, err := iqueue.Dequeue()
	if err != nil {
		logger.Errorf(nil, "err:=[%+v]", err)
	}
	logger.Infof(nil, "val:=[%s]", val)

}

func TestEnqueue4Etcd(t *testing.T) {
	pubsubConnStr := "192.168.0.6:12379"
	pubsubType := "etcd"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr,
	}
	iClient, _ := c.NewIClient(pubsubType, pubsubConfigMap)

	etcdQueue := etcdq.EtcdQueue{}
	var iqueue i.IQueue
	iqueue = &etcdQueue
	iqueue.SetClient(&iClient)
	iqueue.SetTopic("test_topic1")
	iqueue.Enqueue("ssss")
}

func TestDequeue4Etcd(t *testing.T) {
	pubsubConnStr := "192.168.0.6:12379"
	pubsubType := "etcd"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr,
	}
	iClient, _ := c.NewIClient(pubsubType, pubsubConfigMap)

	etcdQueue := etcdq.EtcdQueue{}
	var iqueue i.IQueue
	iqueue = &etcdQueue
	iqueue.SetClient(&iClient)
	iqueue.SetTopic("test_topic1")

	val, err := iqueue.Dequeue()
	if err != nil {
		logger.Errorf(nil, "err:=[%+v]", err)
	}
	logger.Infof(nil, "val:=[%s]", val)
}
