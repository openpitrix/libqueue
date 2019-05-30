package redisqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"openpitrix.io/logger"

	lib "openpitrix.io/libqueue"
)

type client struct {
	*redis.Client
}

func (c client) SetTopic(t string) (lib.Topic, error) {
	return Topic{c.Client, t}, nil
}

type Topic struct {
	*redis.Client
	t string
}

func (t Topic) New(configMap map[string]interface{}) (lib.QueueClient, error) {
	cfg := LoadConf(configMap)

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
	return client{cli}, nil
}

func (t Topic) Enqueue(val string) error {
	_, err := t.LPush(t.t, val).Result()
	return err
}
func (t Topic) Dequeue() (string, error) {
	val, err := t.BRPop(0, t.t).Result()
	if err != nil {
		return "", err
	}

	if len(val) != 2 {
		return "", errors.New("redis dequeue format error")
	}

	return val[1], nil
}

func (t Topic) Topic() string {
	return t.t
}

type RedisConfig struct {
	ConnStr      string
	PoolSize     int
	MinIdleConns int
}

func LoadConf(configMap map[string]interface{}) *RedisConfig {
	mjson, _ := json.Marshal(configMap)
	mString := string(mjson)
	logger.Infof(nil, "mString:%s", mString)

	var config RedisConfig
	data := []byte(mString)
	err := json.Unmarshal(data, &config)
	if err != nil {
		fmt.Println(err)
	}
	return &config
}
