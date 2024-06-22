package main

import (
	"github.com/IBM/sarama"
	"log"
)

func main() {
	brokerList := []string{"localhost:9092"} // kafka broker address

	producer, err := sarama.NewSyncProducer(brokerList, nil)
	if err != nil {
		log.Printf("Failed to start Sarama producer: %v", err)
		return
	}
	defer producer.Close()

	topic := "example_topic" // kafka topic

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("hello world"), // set any string value
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return
	}

	log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
