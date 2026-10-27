package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

type RefundRequestEvent struct {
	EventID string `json:"event_id"`
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type RefundResultEvent struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
}

func consumeRefundRequests(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "refund-requests",
		GroupID:  "payment-service-refunds",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "refund-results",
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	log.Println("Payment Service consuming refund-requests")

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("Failed to read refund request:", err)
			continue
		}

		var request RefundRequestEvent
		if err := json.Unmarshal(message.Value, &request); err != nil {
			log.Println("Invalid refund request:", err)
			continue
		}

		if request.EventID == "" || request.OrderID == "" {
			log.Println("Refund request is missing required IDs")
			continue
		}

		log.Printf("Simulating refund for order %s; reason: %s",
			request.OrderID, request.Reason)

		// Simulate a successful refund for now.
		result := RefundResultEvent{
			EventID:   request.EventID,
			EventType: "RefundSucceeded",
			OrderID:   request.OrderID,
			Status:    "SUCCEEDED",
		}

		payload, err := json.Marshal(result)
		if err != nil {
			log.Println("Failed to encode refund result:", err)
			continue
		}

		if err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(request.OrderID),
			Value: payload,
		}); err != nil {
			log.Println("Failed to publish refund result:", err)
			continue
		}

		log.Printf("Published refund result for order %s: %s",
			request.OrderID, result.Status)
	}
}
