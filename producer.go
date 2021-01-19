package events

import (
	"encoding/json"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func SetKafkaDeliveryReportHandler(p kafka.Producer) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
			}
		}
	}
}

func Produce(p kafka.Producer, topic Topic, event interface{}) {
	eventJson, _ := json.Marshal(event)

	_ = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic.ToString(), Partition: kafka.PartitionAny},
		Value:          []byte(eventJson),
	}, nil)
}

func SetKafkaProducer(err error, servers string) *kafka.Producer {
	fmt.Println("Connecting to kafka producer")
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": servers})
	if err != nil {
		panic(err)
	}

	return p
}
