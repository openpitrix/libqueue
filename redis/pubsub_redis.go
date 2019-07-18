// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package redis

import (
	"github.com/go-redis/redis"
	i "openpitrix.io/libqueue"
)

type RedisPubSub struct {
	RedisClient
	Channel string
}

func (rPubSub *RedisPubSub) SetClient(iClient *i.IClient) i.IPubSub {
	rPubSub.RedisClient = (*iClient).(RedisClient)
	return rPubSub
}

func (rPubSub *RedisPubSub) SetChannel(channel string) i.IPubSub {
	rPubSub.Channel = channel
	return rPubSub
}

func (rPubSub *RedisPubSub) Publish(msg string) error {
	err := (rPubSub.Client).Publish(rPubSub.Channel, msg).Err()
	if err != nil {
		panic(err)
	}
	return nil
}

func (rPubSub *RedisPubSub) ReceiveMessage() chan string {
	var msgChan = make(chan string, 255)
	pubsub := (rPubSub.Client).PSubscribe(rPubSub.Channel)
	go rPubSub.getMessages(pubsub, msgChan)
	return msgChan
}

func (rPubSub *RedisPubSub) getMessages(ps *redis.PubSub, msgChan chan string) {
	ch := ps.Channel()
	for msg := range ch {
		msgChan <- msg.Payload
	}
}

func (rPubSub *RedisPubSub) Close() error {
	err := rPubSub.Client.Close()
	if err != nil {
		return err
	} else {
		return nil
	}
}
