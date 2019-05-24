package mq

type QC interface {
	Connect(connStrs []string) (QC, error)
	GetTopic(string) (Topic, error)
}

type Topic interface {
	Enqueue(string) error
	Dequeue() (string, error)
}
