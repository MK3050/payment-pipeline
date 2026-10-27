package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
)

type OrderCreatedEvent struct {
	ID          string `json:"id"`
	AmountCents int64  `json:"amount_cents"`
	Status      string `json:"status"`
}

type PaymentResultEvent struct {
	EventType   string `json:"event_type"`
	EventID     string `json:"event_id"`
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
	Status      string `json:"status"`
}

func processPayment(payload []byte) (*PaymentResultEvent, error) {
	var order OrderCreatedEvent

	if err := json.Unmarshal(payload, &order); err != nil {
		return nil, err
	}

	if order.ID == "" || order.AmountCents <= 0 {
		return nil, fmt.Errorf("invalid order event")
	}

	// Simulate a successful payment for now.
	result := &PaymentResultEvent{
		EventType:   "PaymentSucceeded",
		EventID:     uuid.New().String(),
		OrderID:     order.ID,
		AmountCents: order.AmountCents,
		Status:      "SUCCEEDED",
	}

	log.Printf("Payment %s for order %s: %s",
		result.EventID, result.OrderID, result.Status)

	return result, nil
}
