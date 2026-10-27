package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func consumeOrders(ctx context.Context, db *sql.DB) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "orders",
		GroupID:        "payment-service",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer reader.Close()

	log.Println("Payment Service consuming orders topic")

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("Failed to read order event:", err)
			continue
		}

		if err := processOrderPayment(ctx, db, message.Value); err != nil {
			log.Println("Failed to persist payment:", err)
			continue
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Println("Failed to commit order event:", err)
			continue
		}
	}
}

func processOrderPayment(
	ctx context.Context,
	db *sql.DB,
	payload []byte,
) error {
	result, err := processPayment(payload)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	insertResult, err := tx.ExecContext(ctx, `
		INSERT INTO payments (
			order_id, event_id, amount_cents, status
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (order_id) DO NOTHING
	`,
		result.OrderID,
		result.EventID,
		result.AmountCents,
		result.Status,
	)
	if err != nil {
		return err
	}

	inserted, err := insertResult.RowsAffected()
	if err != nil {
		return err
	}

	if inserted == 0 {
		log.Printf("Payment already exists for order %s", result.OrderID)
		return tx.Commit()
	}

	result.EventType = "PaymentSucceeded"

	eventPayload, err := json.Marshal(result)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO payment_outbox (
			id, aggregate_id, event_type, payload
		)
		VALUES ($1, $2, $3, $4::jsonb)
	`,
		result.EventID,
		result.OrderID,
		result.EventType,
		string(eventPayload),
	)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Payment saved for order %s", result.OrderID)
	return nil
}

func runRefundOutboxPublisher(ctx context.Context, db *sql.DB) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "refund-results",
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	log.Println("Refund outbox publisher started")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		if err := publishPendingRefundEvents(ctx, db, writer); err != nil {
			log.Println("Refund outbox publishing failed:", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func publishPendingRefundEvents(
	ctx context.Context,
	db *sql.DB,
	writer *kafka.Writer,
) error {
	rows, err := db.QueryContext(ctx, `
		SELECT id, aggregate_id, payload
		FROM refund_outbox
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT 100
	`)
	if err != nil {
		return err
	}

	type pendingEvent struct {
		id, aggregateID string
		payload         []byte
	}

	var events []pendingEvent
	for rows.Next() {
		var event pendingEvent
		if err := rows.Scan(&event.id, &event.aggregateID, &event.payload); err != nil {
			rows.Close()
			return err
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, event := range events {
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.aggregateID),
			Value: event.payload,
		})
		if err != nil {
			return err
		}

		_, err = db.ExecContext(ctx, `
			UPDATE refund_outbox
			SET published_at = NOW()
			WHERE id = $1 AND published_at IS NULL
		`, event.id)
		if err != nil {
			return err
		}

		log.Printf("Published refund result for order %s", event.aggregateID)
	}

	return nil
}
