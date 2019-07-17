// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package redis

import (
	"errors"

	"github.com/go-redis/redis"
	"openpitrix.io/logger"

	i "openpitrix.io/libqueue"
)

type RedisClient struct {
	redis.Client
}

type RedisQueue struct {
	RedisClient
	Topic string
}

func (rq *RedisQueue) SetTopic(topic string) i.IQueue {
	rq.Topic = topic
	return rq
}

func (rq *RedisQueue) SetClient(iClient *i.IClient) i.IQueue {
	rq.RedisClient = (*iClient).(RedisClient)
	return rq
}

func (rq *RedisQueue) Enqueue(val string) error {
	_, err := rq.LPush(rq.Topic, val).Result()
	if err != nil {
		logger.Errorf(nil, "Enqueue from redis queue failded,err=%+v", err)
	}
	return err
}

func (rq *RedisQueue) Dequeue() (string, error) {
	val, err := rq.BRPop(0, rq.Topic).Result()
	if err != nil {
		logger.Errorf(nil, "Enqueue from redis queue failded,err=%+v", err)
		return "", err
	}

	if len(val) != 2 {
		logger.Errorf(nil, "redis dequeue format error")
		return "", errors.New("redis dequeue format error")
	}

	return val[1], nil
}
