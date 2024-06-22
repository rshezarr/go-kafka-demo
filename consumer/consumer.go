package main

import (
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
)

func main() {
	brokerList := []string{"localhost:9092"} // kafka broker address

	topic := "example_topic"

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// create a new consumer
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		log.Fatalf("Failed to start consumer: %s", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start partition consumer: %s", err)
	}
	defer partitionConsumer.Close()

	// trap SIGINT to trigger a graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-partitionConsumer.Errors():
				log.Printf("ERROR: %s", err.Error())
			case msg := <-partitionConsumer.Messages():
				log.Printf("Received message on topic: %s, Value: %s", topic, string(msg.Value))
			// process message here
			case <-signals:
				log.Printf("Interrupt signal received")
				doneCh <- struct{}{}
				return
			}
		}
	}()

	log.Println("Consumer started...")
	<-doneCh
	log.Println("Consumer shutting down...")
}
