package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

func consumePaymentResults(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		CommitInterval: 0,
		Brokers:        []string{"localhost:9092"},
		Topic:          "payment-results",
		GroupID:        "inventory-service",
		MinBytes:       1,
		MaxBytes:       10e6,
	})
	defer reader.Close()

	db, err := connectDB()
	if err != nil {
		log.Println("Inventory database connection failed:", err)
		return
	}
	defer db.Close()

	log.Println("Inventory Service consuming payment-results")

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("Failed to read payment result:", err)
			continue
		}

		var payment PaymentResultEvent
		if err := json.Unmarshal(message.Value, &payment); err != nil {
			log.Println("Invalid payment result:", err)
			continue
		}

		if payment.Status != "SUCCEEDED" {
			log.Printf("Skipping inventory reservation for order %s: payment status %s",
				payment.OrderID, payment.Status)
			continue
		}

		if payment.EventID == "" || payment.OrderID == "" {
			log.Println("Payment result is missing event_id or order_id")
			continue
		}

		if err := processPaymentResult(ctx, db, payment); err != nil {
			log.Printf("Failed to process payment event %s: %v",
				payment.EventID, err)
			continue
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("Failed to commit payment event %s: %v",
				payment.EventID, err)
			continue
		}
		if payment.Status != "SUCCEEDED" {
			log.Printf("Skipping inventory reservation for order %s: payment status %s",
				payment.OrderID, payment.Status)

			if err := reader.CommitMessages(ctx, message); err != nil {
				log.Println("Failed to commit skipped payment event:", err)
			}
			continue
		}
	}
}

func processPaymentResult(
	ctx context.Context,
	db *sql.DB,
	payment PaymentResultEvent,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Record this event. A duplicate event_id will not be processed again.
	inserted, err := tx.ExecContext(ctx, `
		INSERT INTO inventory_processed_events (event_id, order_id, status)
		VALUES ($1, $2, 'PROCESSING')
		ON CONFLICT (event_id) DO NOTHING
	`, payment.EventID, payment.OrderID)
	if err != nil {
		return err
	}

	rows, err := inserted.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		log.Printf("Ignoring duplicate payment event %s", payment.EventID)
		return nil
	}

	result := InventoryResultEvent{
		EventID:   payment.EventID,
		OrderID:   payment.OrderID,
		EventType: "InventoryReserved",
		Status:    "SUCCEEDED",
	}

	// Reserve one unit of product-1, atomically.
	stockResult, err := tx.ExecContext(ctx, `
		UPDATE inventory_stock
		SET quantity = quantity - 1,
		    updated_at = NOW()
		WHERE product_id = 'product-1'
		  AND quantity >= 1
	`)
	if err != nil {
		return err
	}

	affected, err := stockResult.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		result.EventType = "InventoryFailed"
		result.Status = "FAILED"
		result.Reason = "Insufficient stock or product not found"
	}

	// Record the final outcome in the same transaction.
	_, err = tx.ExecContext(ctx, `
		UPDATE inventory_processed_events
		SET status = $1
		WHERE event_id = $2
	`, result.Status, payment.EventID)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}

	// Persist the Kafka event before attempting to publish it.
	_, err = tx.ExecContext(ctx, `
		INSERT INTO inventory_outbox (id, aggregate_id, event_type, payload)
		VALUES ($1, $2, $3, $4::jsonb)
	`, payment.EventID, payment.OrderID, result.EventType, string(payload))
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Order %s inventory result saved: %s",
		payment.OrderID, result.Status)

	return nil
}
