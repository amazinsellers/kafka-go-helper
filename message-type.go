package events

type MessageTypes struct {
	LOGGING   MessageType
	QUEUING   MessageType
	TRACKING  MessageType
	ETL       MessageType
	STREAMING MessageType
	PUSH      MessageType
	USER      MessageType
}

func NewMessageTypes() MessageTypes {
	return MessageTypes{
		LOGGING:   "logging",
		QUEUING:   "queuing",
		TRACKING:  "tracking",
		ETL:       "etl",
		STREAMING: "streaming",
		PUSH:      "push",
		USER:      "user",
	}
}

type MessageType string
