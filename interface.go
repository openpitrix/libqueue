package libqueue

type QueueClient interface {
	GetTopic(string) (Topic, error)
}

type Topic interface {
	Enqueue(string) error
	Dequeue() (string, error)
	Topic() string
}
