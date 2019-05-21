package lib

import (
	"errors"
	"github.com/go-redis/redis"
)

type RedisClient struct {
	*redis.Client
}

type RedisQueue struct {
	client *redis.Client
	topic  string
}

func (client *RedisClient) NewQueue(topic string) *RedisQueue {
	return &RedisQueue{client.Client, topic}
}

func (q *RedisQueue) Enqueue(val string) error {
	_, err := q.client.LPush(q.topic, val).Result()
	return err
}

// Dequeue returns Enqueue()'d elements in FIFO order. If the
// queue is empty, Dequeue blocks until elements are available.
func (q *RedisQueue) Dequeue() (string, error) {
	val, err := q.client.BRPop(0, q.topic).Result()
	if err != nil {
		return "", err
	}

	if len(val) != 2 {
		return "", errors.New("redis dequeue format error")
	}

	return val[1], nil
}

func Connect(addr string, password string, poolSize int) (*RedisClient, error) {
	cli := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
		PoolSize: poolSize,
	})
	return &RedisClient{cli}, nil
}
