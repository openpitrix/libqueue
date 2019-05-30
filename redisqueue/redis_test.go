package redisqueue

import (
	"testing"

	"openpitrix.io/logger"
)

func TestRedis(t *testing.T) {
	connStr := "redis://192.168.0.4:6379"
	var configMap map[string]interface{}
	configMap = make(map[string]interface{})
	configMap["connStr"] = connStr
	configMap["PoolSize"] = 2000
	//configMap["MinIdleConns"] = 1

	redisTopic := new(Topic)
	c, err := redisTopic.New(configMap)
	if err != nil {
		logger.Errorf(nil, "err:=[%+v]", err)
	}

	topic, err := c.SetTopic("sss")
	if err != nil {
		logger.Errorf(nil, "err:=[%+v]", err)
	}
	err = topic.Enqueue("hello")
	if err != nil {
		logger.Errorf(nil, "err:=[%+v]", err)
	}
	val, err := topic.Dequeue()
	if err != nil {
		logger.Errorf(nil, "err:=[%+v]", err)
	}
	logger.Infof(nil, "val:=[%s]", val)
}

func TestLoadConf(t *testing.T) {
	m := map[string]interface{}{"PoolSize": 200, "MinIdleConns": 10}
	cfg := LoadConf(m)
	logger.Infof(nil, "ConnStr:%s", cfg.ConnStr)
	logger.Infof(nil, "PoolSize:%d", cfg.PoolSize)
	logger.Infof(nil, "MinIdleConns:%d", cfg.MinIdleConns)
}
