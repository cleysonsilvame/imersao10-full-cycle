package main

import (
	"log"

	"github.com/cleysonsilvame/imersao10-full-cycle/simulator/application/kafka"
	infraKafka "github.com/cleysonsilvame/imersao10-full-cycle/simulator/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()

	if err != nil {
		log.Fatal("error loading .env file")
	}
}

func main() {
	msgChan := make(chan *ckafka.Message)

	consumer := infraKafka.NewKafkaConsumer(msgChan)

	go consumer.Consume()

	for msg := range msgChan {
		go kafka.Produce(msg)
	}
}
