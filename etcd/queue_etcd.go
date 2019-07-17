// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package etcd

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/contrib/recipes"

	i "openpitrix.io/libqueue"
)

type EtcdClient struct {
	clientv3.Client
}

type EtcdQueue struct {
	EtcdClient
	Topic string
}

func (eq *EtcdQueue) SetTopic(topic string) i.IQueue {
	eq.Topic = topic
	return eq
}

func (eq *EtcdQueue) Enqueue(val string) error {
	etcdQueue := recipe.NewQueue(&eq.EtcdClient.Client, eq.Topic)
	return etcdQueue.Enqueue(val)
}

func (eq *EtcdQueue) Dequeue() (string, error) {
	etcdQueue := recipe.NewQueue(&eq.EtcdClient.Client, eq.Topic)
	return etcdQueue.Dequeue()
}

func (eq *EtcdQueue) SetClient(iClient *i.IClient) i.IQueue {
	eq.EtcdClient = (*iClient).(EtcdClient)
	return eq
}
