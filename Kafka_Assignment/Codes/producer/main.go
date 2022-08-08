package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func main() {

	// w := &kafka.Writer{
	// 	Addr:                   kafka.TCP("localhost:9092"),
	// 	Topic:                  "my-topic123",
	// 	RequiredAcks:           kafka.RequireAll,
	// 	AllowAutoTopicCreation: true,
	// 	Async:                  true,
	// 	Completion: func(messages []kafka.Message, err error) {

	// 		if err != nil {
	// 			fmt.Println(err)
	// 			return
	// 		}

	// 		for _, val := range messages {
	// 			fmt.Printf("messages sent, offset %d, key %s, val %s \n", val.Offset, val.Key, val.Value)
	// 		}
	// 	},
	// }

	// err := w.WriteMessages(context.Background(),
	// 	kafka.Message{
	// 		Key:   []byte("key"),
	// 		Value: []byte("value1"),
	// 	},
	// 	kafka.Message{
	// 		Key:   []byte("key"),
	// 		Value: []byte("value2"),
	// 	},
	// 	kafka.Message{
	// 		Key:   []byte("key"),
	// 		Value: []byte("value3"),
	// 	},
	// )

	// if err != nil {
	// 	log.Fatal("failed to write messages:", err)
	// }

	// if err := w.Close(); err != nil {
	// 	log.Fatal("failed to close writer:", err)
	// }

	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "example-123",
		RequiredAcks:           kafka.RequireAll,
		Async:                  true,
		AllowAutoTopicCreation: true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				fmt.Println(err)
				return
			}

			for _, val := range messages {
				fmt.Printf("Message sent, offset:%d, key:%s, value:%s \n", val.Offset, val.Key, val.Value)
			}
		},
	}

	// err := w.WriteMessages(context.Background(),
	// 	kafka.Message{
	// 		Key:   []byte("key"),
	// 		Value: []byte("value1"),
	// 	},
	// 	kafka.Message{
	// 		Key:   []byte("key"),
	// 		Value: []byte("value2"),
	// 	},
	// 	kafka.Message{
	// 		Key:   []byte("key"),
	// 		Value: []byte("value3"),
	// 	},
	// )

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("key1"),
			Value: []byte("Jyotisman1"),
		},
		kafka.Message{
			Key:   []byte("key2"),
			Value: []byte("Jyotisman2"),
		},
		kafka.Message{
			Key:   []byte("key3"),
			Value: []byte("Jyotisman3"),
		},
	)

	if err != nil {
		log.Fatal("Failed to write message:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("Failed to close writer:", err)
	}
}
