package etcdqueue

import (
	"testing"

	"openpitrix.io/logger"
)

func TestEtcd(t *testing.T) {
	connStr := "192.168.0.6:12379"
	var configMap map[string]interface{}
	configMap = make(map[string]interface{})

	configMap["connStr"] = connStr
	configMap["DialTimeout"] = 8
	etcdTopic := new(Topic)
	c, err := etcdTopic.New(configMap)
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
	m := map[string]interface{}{"DialTimeoutSecond": 6}
	cfg := LoadConf(m)
	logger.Infof(nil, "DialTimeout:%d", cfg.DialTimeoutSecond)
}
