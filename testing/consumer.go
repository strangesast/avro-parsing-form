package main

import (
	"errors"
	"github.com/Shopify/sarama"
	"log"
)

var pointsInputTopic = "points-input"

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewClient([]string{"localhost:9092"}, config)

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	availableTopics, err := consumer.Topics()
	if err != nil {
		panic(err)
	}

	if inSlice(availableTopics, pointsInputTopic) == false {
		panic(errors.New("points input topic not created"))
	}

	partitionConsumer, err := consumer.ConsumePartition(pointsInputTopic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	consumed := 0

	for msg := range partitionConsumer.Messages() {
		log.Printf("Consumed message offset %d (consumed %d)\n", msg.Offset, consumed)
		consumed++
	}

	log.Printf("Consumed: %d\n", consumed)
}

func inSlice(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
