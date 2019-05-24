package mq

import "testing"

func TestRedis(t *testing.T) {
	connStr := []string{"192.168.0.4:6379", "", "2000"}
	q := new(Redis)
	c, _ := q.Connect(connStr)
	topic, _ := c.(*Redis).GetTopic("test")
	topic.Enqueue("sss")
	topic.Dequeue()
}
