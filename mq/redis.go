package mq

import (
	"errors"
	"strconv"

	"github.com/go-redis/redis"
	"openpitrix.io/logger"
)

type Redis struct {
	*redis.Client
}

func (c Redis) Connect(connStrs []string) (QC, error) {
	addr := connStrs[0]
	password := connStrs[1]
	poolSize, _ := strconv.Atoi(connStrs[2])

	cli := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
		PoolSize: poolSize,
	})
	logger.Infof(nil, "Connect done.")
	c.Client = cli
	return &Redis{cli}, nil

}

func (c Redis) GetTopic(topic string) (Topic, error) {
	return &RedisTopic{c.Client, topic}, nil
}

type RedisTopic struct {
	*redis.Client
	topic string
}

func (t RedisTopic) Enqueue(val string) error {
	_, err := t.LPush(t.topic, val).Result()
	return err
}
func (t RedisTopic) Dequeue() (string, error) {
	val, err := t.BRPop(0, t.topic).Result()
	if err != nil {
		return "", err
	}

	if len(val) != 2 {
		return "", errors.New("redis dequeue format error")
	}

	return val[1], nil
}
