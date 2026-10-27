package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

type PaymentResultEvent struct {
	EventID string `json:"event_id"`
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

func consumePaymentResults(ctx context.Context, db *sql.DB) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "payment-results",
		GroupID:  "order-service",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("Order Service consuming payment-results topic")

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("Failed to read payment result:", err)
			continue
		}

		var event PaymentResultEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Println("Invalid payment result:", err)
			continue
		}

		var newStatus string
		switch event.Status {
		case "SUCCEEDED":
			newStatus = "PAYMENT_SUCCEEDED"
		case "FAILED":
			newStatus = "PAYMENT_FAILED"
		default:
			log.Println("Unknown payment status:", event.Status)
			continue
		}

		result, err := db.ExecContext(ctx, `
			UPDATE orders
			SET status = $1
			WHERE id = $2 AND status = 'PENDING'
		`, newStatus, event.OrderID)
		if err != nil {
			log.Println("Failed to update order:", err)
			continue
		}

		count, err := result.RowsAffected()
		if err != nil {
			log.Println("Failed to check update result:", err)
			continue
		}

		if count == 0 {
			log.Printf("Order %s was not updated; it may not exist or may already be processed", event.OrderID)
			continue
		}

		log.Printf("Order %s status updated to %s", event.OrderID, newStatus)
	}
}
