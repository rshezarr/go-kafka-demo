package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)
	if err != nil {
		log.Fatalf("Error while dialing: %v", err)
	}

	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))

	conn.WriteMessages(kafka.Message{Value: []byte("Hello")})
}
