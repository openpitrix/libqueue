package redisqueue

import (
	"openpitrix.io/logger"
	"testing"
)

func TestRedis(t *testing.T) {
	connStrs := "redis://192.168.0.4:6379"
	c, err := New(connStrs)
	if err != nil {
		logger.Errorf(nil, "err:=[%+v]", err)
	}

	topic, err := c.GetTopic("sss")
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
