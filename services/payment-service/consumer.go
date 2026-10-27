package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

func consumeOrders(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "orders",
		GroupID:  "payment-service",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "payment-results",
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	log.Println("Payment Service consuming orders topic")

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("Failed to read Kafka message:", err)
			continue
		}

		result, err := processPayment(message.Value)
		if err != nil {
			log.Println("Failed to process order event:", err)
			continue
		}

		payload, err := json.Marshal(result)
		if err != nil {
			log.Println("Failed to encode payment result:", err)
			continue
		}

		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(result.OrderID),
			Value: payload,
		})
		if err != nil {
			log.Println("Failed to publish payment result:", err)
			continue
		}

		log.Printf("Published %s for order %s",
			result.EventType, result.OrderID)
	}
}
