package events

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"time"
)

type TopicHandlers map[string]func([]byte) error

type KafkaConsumer struct {
	Consumer      *kafka.Consumer
	TopicHandlers TopicHandlers
}

func NewKafkaConsumer(servers string, groupId string) (consumer *KafkaConsumer, err error) {
	consumer = &KafkaConsumer{}
	fmt.Println("Connecting to kafka")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": servers,
		"group.id":          groupId,
		"auto.offset.reset": "earliest",
	})

	consumer.Consumer = c

	if err != nil {
		panic(err)
	}

	return consumer, err
}

func (c KafkaConsumer) Consume() {
	for {
		time.Sleep(time.Duration(2 * time.Second))
		msg, err := c.Consumer.ReadMessage(-1)

		if err != nil {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}

		if aHandler, ok := c.TopicHandlers[*msg.TopicPartition.Topic]; ok {
			fmt.Printf("Handling message for topic \"%s\": %s\n", *msg.TopicPartition.Topic, string(msg.Value))
			_ = aHandler(msg.Value)
			continue
		}

		fmt.Printf("No handlers found for Topic %s\n", *msg.TopicPartition.Topic)
	}
}

func (c KafkaConsumer) Subscribe() {
	topics := make([]string, len(c.TopicHandlers))

	i := 0
	for k := range c.TopicHandlers {
		topics[i] = k
		i++
	}

	c.Consumer.SubscribeTopics(topics, nil)
}
