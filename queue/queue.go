package queue

import (
	"fmt"

	lib "openpitrix.io/libqueue"
	e "openpitrix.io/libqueue/etcdqueue"
	r "openpitrix.io/libqueue/redisqueue"
)

func New(queueType string, configMap map[string]interface{}) (lib.QueueClient, error) {
	if configMap == nil {
		return nil, fmt.Errorf("not provide queue configuration info.")
	}

	switch queueType {
	case "etcd":
		etcdTopic := new(e.Topic)
		c, _ := etcdTopic.New(configMap)
		return c, nil
	case "redis":
		redisTopic := new(r.Topic)
		c, _ := redisTopic.New(configMap)
		return c, nil
	default:
		return nil, fmt.Errorf("unsupported queueType [%s]", queueType)
	}
}
