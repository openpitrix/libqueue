// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package etcd

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"openpitrix.io/logger"

	i "openpitrix.io/libqueue"
	"openpitrix.io/libqueue/util"
)

type EtcdPubSub struct {
	EtcdClient
	Channel string
}

func (ePubSub *EtcdPubSub) SetClient(iClient *i.IClient) i.IPubSub {
	ePubSub.EtcdClient = (*iClient).(EtcdClient)
	return ePubSub
}

func (ePubSub *EtcdPubSub) SetChannel(channel string) i.IPubSub {
	ePubSub.Channel = channel
	return ePubSub
}

func (ePubSub *EtcdPubSub) Publish(msg string) error {
	cli := ePubSub.EtcdClient.Client
	resp, err := cli.Grant(context.Background(), 60)
	if err != nil {
		logger.Errorf(nil, "Grant ttl from etcd failed: %+v", err)
		return err
	}
	var msgId = util.GetIntId()
	key := fmt.Sprintf("%s/%d", ePubSub.Channel, msgId)

	_, err = cli.Put(context.Background(), key, msg, clientv3.WithLease(resp.ID))
	if err != nil {
		logger.Errorf(nil, "Push message[%+v] to etcd failed: %+v", msg, err)
		return err
	}

	logger.Debugf(nil, "Push message[%+v] to etcd successfully.", msg)
	return nil
}

func (ePubSub *EtcdPubSub) ReceiveMessage() chan string {
	var msgChan = make(chan string, 255)
	client := ePubSub.EtcdClient
	go ePubSub.getMessage(&client, msgChan)
	return msgChan
}

func (ePubSub *EtcdPubSub) getMessage(e *EtcdClient, msgChan chan string) {
	key := ePubSub.Channel
	watchRes := e.Watch(context.Background(), key+"/", clientv3.WithPrefix())
	for res := range watchRes {
		for _, ev := range res.Events {
			if ev.Type == mvccpb.PUT {
				msgChan <- string(ev.Kv.Value)
			}
		}
	}
}

func (ePubSub *EtcdPubSub) Close() error {
	err := ePubSub.Client.Close()
	if err != nil {
		return err
	} else {
		return nil
	}
}
