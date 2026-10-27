package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

type RefundResultEvent struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
}

func consumeRefundResults(ctx context.Context, db *sql.DB) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "refund-results",
		GroupID:  "order-service-refunds",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("Order Service consuming refund-results")

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("Failed to read refund result:", err)
			continue
		}

		var event RefundResultEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Println("Invalid refund result:", err)
			continue
		}

		var newStatus string
		switch event.Status {
		case "SUCCEEDED":
			newStatus = "CANCELLED_REFUNDED"
		case "FAILED":
			newStatus = "REFUND_FAILED"
		default:
			log.Println("Unknown refund status:", event.Status)
			continue
		}

		result, err := db.ExecContext(ctx, `
			UPDATE orders
			SET status = $1
			WHERE id = $2 AND status = 'INVENTORY_FAILED'
		`, newStatus, event.OrderID)
		if err != nil {
			log.Println("Failed to update order from refund result:", err)
			continue
		}

		count, err := result.RowsAffected()
		if err != nil {
			log.Println("Failed to check refund update:", err)
			continue
		}
		if count == 0 {
			log.Printf("Order %s was not updated; it may not be awaiting a refund", event.OrderID)
			continue
		}

		log.Printf("Order %s status updated to %s", event.OrderID, newStatus)
	}
}
