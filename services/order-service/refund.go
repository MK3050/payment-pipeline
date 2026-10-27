package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type RefundRequestEvent struct {
	EventID string `json:"event_id"`
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

func publishRefundRequest(ctx context.Context, orderID, reason string) error {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "refund-requests",
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	event := RefundRequestEvent{
		EventID: uuid.NewString(),
		OrderID: orderID,
		Reason:  reason,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(orderID),
		Value: payload,
	})
	if err != nil {
		return err
	}

	log.Printf("Published refund request for order %s", orderID)
	return nil
}
