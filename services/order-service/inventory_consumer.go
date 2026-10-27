package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

type InventoryResultEvent struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
}

func consumeInventoryResults(ctx context.Context, db *sql.DB) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "inventory-results",
		GroupID:  "order-service-inventory",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("Order Service consuming inventory-results")

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("Failed to read inventory result:", err)
			continue
		}

		var event InventoryResultEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Println("Invalid inventory result:", err)
			continue
		}

		var newStatus string
		switch event.Status {
		case "SUCCEEDED":
			newStatus = "CONFIRMED"
		case "FAILED":
			newStatus = "INVENTORY_FAILED"
		default:
			log.Println("Unknown inventory status:", event.Status)
			continue
		}

		result, err := db.ExecContext(ctx, `
			UPDATE orders
			SET status = $1
			WHERE id = $2 AND status = 'PAYMENT_SUCCEEDED'
		`, newStatus, event.OrderID)
		if err != nil {
			log.Println("Failed to update order from inventory result:", err)
			continue
		}

		count, err := result.RowsAffected()
		if err != nil {
			log.Println("Failed to check inventory update:", err)
			continue
		}
		if count == 0 {
			log.Printf("Order %s was not updated; it may not be awaiting inventory", event.OrderID)
			continue
		}
		if event.Status == "FAILED" {
			if err := publishRefundRequest(
				ctx,
				event.OrderID,
				event.Reason,
			); err != nil {
				log.Printf(
					"Failed to publish refund request for order %s: %v",
					event.OrderID,
					err,
				)
			}
		}
		log.Printf("Order %s status updated to %s", event.OrderID, newStatus)
	}
}
