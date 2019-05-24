package mq

import "testing"

func TestEtcd(t *testing.T) {
	connStrs := []string{"192.168.0.6:12379"}
	etcd := new(Etcd)
	c, _ := etcd.Connect(connStrs)
	topic, _ := c.(*Etcd).GetTopic("test")
	topic.Enqueue("sss")
	topic.Dequeue()

}
