package main

import (
	"consumer/config"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/Trendyol/kafka-konsumer"
)

func main() {
	cfg := config.GetConfig()

	consumerCfg := &kafka.ConsumerConfig{
		ClientID:    cfg.Kafka.ClientId,
		Concurrency: 1,
		Reader: kafka.ReaderConfig{
			Brokers: strings.Split(cfg.Kafka.Brokers, ","),
			Topic:   cfg.Kafka.Topic,
			GroupID: cfg.Kafka.GroupId,
		},
		RetryEnabled: false,
		ConsumeFn:    consumeFn,
	}

	consumer, _ := kafka.NewConsumer(consumerCfg)
	defer consumer.Stop()

	consumer.Consume()

	fmt.Println("Consumer started...!")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func consumeFn(message kafka.Message) error {
	fmt.Println(fmt.Sprintf("Message From %s with value %s", message.Topic, string(message.Value)))
	return nil
}
