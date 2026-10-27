package main

import (
	"context"
	"database/sql"
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
		Brokers:        []string{"localhost:9092"},
		Topic:          "refund-requests",
		GroupID:        "payment-service-refunds",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer reader.Close()

	db, err := connectDB()
	if err != nil {
		log.Println("Refund database connection failed:", err)
		return
	}
	defer db.Close()

	log.Println("Payment Service consuming refund-requests")

	for {
		message, err := reader.FetchMessage(ctx)
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

		if err := processRefundRequest(ctx, db, request); err != nil {
			log.Println("Failed to persist refund:", err)
			continue
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Println("Failed to commit refund request:", err)
		}
	}
}

func processRefundRequest(
	ctx context.Context,
	db *sql.DB,
	request RefundRequestEvent,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result := RefundResultEvent{
		EventID:   request.EventID,
		EventType: "RefundSucceeded",
		OrderID:   request.OrderID,
		Status:    "SUCCEEDED",
	}

	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}

	inserted, err := tx.ExecContext(ctx, `
		INSERT INTO refunds (order_id, event_id, status)
		VALUES ($1, $2, $3)
		ON CONFLICT (order_id) DO NOTHING
	`, request.OrderID, request.EventID, result.Status)
	if err != nil {
		return err
	}

	rows, err := inserted.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		log.Printf("Refund already recorded for order %s", request.OrderID)
		return tx.Commit()
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO refund_outbox (id, aggregate_id, event_type, payload)
		VALUES ($1, $2, $3, $4::jsonb)
	`, request.EventID, request.OrderID, result.EventType, string(payload))
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Refund result saved for order %s", request.OrderID)
	return nil
}
