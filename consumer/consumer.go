package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)
	if err != nil {
		log.Fatalf("Error while dialing: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second * 8)); err != nil {
		log.Fatalf("Error while setting read deadline: %v", err)
	}

	batch := conn.ReadBatch(1e3, 1e9) // 1KB, 1GB

	bytes := make([]byte, 1e3)

	for {
		_, err := batch.Read(bytes)
		if err != nil {
			break
		}
		fmt.Println(string(bytes))
	}
}
