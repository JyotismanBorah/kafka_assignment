package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	topic := "example-123"

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "group-1",
		Topic:   topic,
	})

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("Reading messages at offset %d: %s = %s\n", msg.Offset, string(msg.Key), string(msg.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("Unable to close reader:", err)
	}
}
