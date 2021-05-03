package events

import (
	"encoding/json"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"time"
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

func Produce(p kafka.Producer, topic Topic, event interface{}) error {
	eventJson, _ := json.Marshal(event)

	err := error(nil)

	for i := 0; i < 3; i++ {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: topic.ToString(), Partition: kafka.PartitionAny},
			Value:          []byte(eventJson),
		}, nil)

		if err == nil {
			return nil
		}

		fmt.Println(fmt.Sprintf("[producer.Produce] retry attempt: %d / error producing event: %s",
			i+1, err.Error()))

		retryIn := time.Duration((i+1) * 3)
		time.Sleep(retryIn * time.Second)
	}

	fmt.Println(fmt.Sprintf("[producer.Produce] retry attempts failed / error producing event: %s",
		err.Error()))
	return err
}

func SetKafkaProducer(err error, servers string) *kafka.Producer {
	fmt.Println("Connecting to kafka producer")
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": servers})
	if err != nil {
		panic(err)
	}

	return p
}
