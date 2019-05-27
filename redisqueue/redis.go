package redisqueue

import (
	"errors"

	"github.com/go-redis/redis"

	lib "openpitrix.io/libqueue"
)

type client struct {
	*redis.Client
}

func New(connStr string) (lib.QueueClient, error) {
	options, err := redis.ParseURL(connStr)
	if err != nil {
		return nil, err
	}
	cli := redis.NewClient(options)

	return client{cli}, nil
}

func (c client) GetTopic(t string) (lib.Topic, error) {
	return topic{c.Client, t}, nil
}

type topic struct {
	*redis.Client
	t string
}

func (t topic) Enqueue(val string) error {
	_, err := t.LPush(t.t, val).Result()
	return err
}
func (t topic) Dequeue() (string, error) {
	val, err := t.BRPop(0, t.t).Result()
	if err != nil {
		return "", err
	}

	if len(val) != 2 {
		return "", errors.New("redis dequeue format error")
	}

	return val[1], nil
}

func (t topic) Topic() string {
	return t.t
}
