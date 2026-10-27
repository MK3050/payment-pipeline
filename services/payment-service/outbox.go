package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func runPaymentOutboxPublisher(ctx context.Context, db *sql.DB) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "payment-results",
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	log.Println("Payment outbox publisher started")

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		if err := publishPendingPaymentEvents(ctx, db, writer); err != nil {
			log.Println("Payment outbox publishing failed:", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func publishPendingPaymentEvents(
	ctx context.Context,
	db *sql.DB,
	writer *kafka.Writer,
) error {
	rows, err := db.QueryContext(ctx, `
		SELECT id, aggregate_id, payload
		FROM payment_outbox
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT 100
	`)
	if err != nil {
		return err
	}

	type pendingEvent struct {
		id          string
		aggregateID string
		payload     []byte
	}

	var events []pendingEvent

	for rows.Next() {
		var event pendingEvent
		if err := rows.Scan(
			&event.id,
			&event.aggregateID,
			&event.payload,
		); err != nil {
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
			UPDATE payment_outbox
			SET published_at = NOW()
			WHERE id = $1 AND published_at IS NULL
		`, event.id)
		if err != nil {
			return err
		}

		log.Printf("Published payment event %s", event.id)
	}

	return nil
}
