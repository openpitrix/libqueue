package etcdqueue

import "testing"

func TestEtcd(t *testing.T) {
	connStr := "192.168.0.6:12379"
	c, _ := New(connStr)
	topic, _ := c.GetTopic("sss")
	topic.Enqueue("hello")
	topic.Dequeue()
}
