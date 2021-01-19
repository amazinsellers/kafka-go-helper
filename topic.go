package events

import "fmt"

type Topic struct {
	MessageType MessageType
	DatasetName string
	DataName    string
}

func NewTopic(msgType MessageType, datasetName string, dataName string) *Topic {
	aTopic := Topic{
		DatasetName: datasetName,
		DataName:    dataName,
	}

	aTopic.MessageType = msgType

	return &aTopic
}

func (o Topic) ToString() *string {
	topic := fmt.Sprintf("%s.%s.%s", o.MessageType, o.DatasetName, o.DataName)
	return &topic
}
