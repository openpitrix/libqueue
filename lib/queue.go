package lib

import (
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/go-redis/redis"
)

type QCQueue struct {
	queueType string
	connStrs  []string
	topic     string
	client    QCClient
}

type QCClient interface{}

func (q *QCQueue) Connect() (QCClient, error) {
	if q.client != nil {
		switch q.queueType {
		case "etcd":
			cli := q.client.(*clientv3.Client)
			q.client = cli
			return cli, nil
		case "redis":
			cli := q.client.(*redis.Client)
			q.client = cli
			return cli, nil
		default:
			return nil, fmt.Errorf("unsupported queueType [%s]", q.queueType)
		}
	} else {
		switch q.queueType {
		case "etcd":
			etcdQueue := new(EtcdQueue)
			cli, err := etcdQueue.Connect(q.connStrs)
			if err != nil {
				return nil, err
			}
			q.client = cli
			return cli, nil
		case "redis":
			redisQueue := new(RedisQueue)
			cli, err := redisQueue.Connect(q.connStrs)
			if err != nil {
				return nil, err
			}
			q.client = cli
			return cli, nil
		default:
			return nil, fmt.Errorf("unsupported queueType [%s]", q.queueType)
		}
	}

}

func (q *QCQueue) NewQueue(client QCClient, topic string) error {
	switch q.queueType {
	case "etcd":
		etcdQueue := new(EtcdQueue)
		etcdQueue.topic = topic
		etcdCli := client.(*clientv3.Client)
		etcdQueue.NewQueue(etcdCli, topic)
	case "redis":
		redisQueue := new(RedisQueue)
		redisQueue.topic = topic
		redisCli := client.(*redis.Client)
		redisQueue.NewQueue(redisCli, topic)
	default:
		return fmt.Errorf("unsupported queueType [%s]", q.queueType)
	}
	return nil

}

func (q *QCQueue) Enqueue(cli QCClient, val string) error {
	switch q.queueType {
	case "etcd":
		etcdQueue := new(EtcdQueue)
		etcdQueue.topic = q.topic
		etcdCli := cli.(*clientv3.Client)
		err := etcdQueue.Enqueue(etcdCli, val)
		if err != nil {
			return err
		}
	case "redis":
		redisQueue := new(RedisQueue)
		redisCli := cli.(*redis.Client)
		redisQueue.topic = q.topic
		err := redisQueue.Enqueue(redisCli, val)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported queueType [%s]", q.queueType)
	}
	return nil
}

func (q *QCQueue) Dequeue(cli QCClient) (string, error) {
	switch q.queueType {
	case "etcd":
		etcdQueue := new(EtcdQueue)
		etcdCli := cli.(*clientv3.Client)
		etcdQueue.topic = q.topic

		val, err := etcdQueue.Dequeue(etcdCli)
		if err != nil {
			return "", err
		}
		return val, err
	case "redis":
		redisQueue := new(RedisQueue)
		redisCli := cli.(*redis.Client)
		redisQueue.topic = q.topic
		val, err := redisQueue.Dequeue(redisCli)
		if err != nil {
			return "", err
		}
		return val, err
	default:
		return "", fmt.Errorf("unsupported queueType [%s]", q.queueType)
	}

}
