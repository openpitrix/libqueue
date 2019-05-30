package libqueue

type QueueClient interface {
	SetTopic(string) (Topic, error)
}

type Topic interface {
	Enqueue(string) error
	Dequeue() (string, error)
	Topic() string
	New(configMap map[string]interface{}) (QueueClient, error)
}
