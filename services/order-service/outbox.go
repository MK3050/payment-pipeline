package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type OutboxEvent struct {
	ID          string
	AggregateID string
	Payload     []byte
}

func runOutboxPublisher(ctx context.Context, db *sql.DB) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "orders",
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publishPendingEvents(ctx, db, writer)
		}
	}
}

func publishPendingEvents(
	ctx context.Context,
	db *sql.DB,
	writer *kafka.Writer,
) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, aggregate_id, payload
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT 100
	`)
	if err != nil {
		log.Println("Failed to query outbox:", err)
		return
	}

	var events []OutboxEvent

	for rows.Next() {
		var event OutboxEvent

		if err := rows.Scan(
			&event.ID,
			&event.AggregateID,
			&event.Payload,
		); err != nil {
			log.Println("Failed to scan outbox event:", err)
			rows.Close()
			return
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		log.Println("Failed while reading outbox:", err)
		rows.Close()
		return
	}
	rows.Close()

	for _, event := range events {
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.AggregateID),
			Value: event.Payload,
		})
		if err != nil {
			log.Println("Failed to publish event:", err)
			return
		}

		_, err = db.ExecContext(ctx, `
			UPDATE outbox_events
			SET published_at = NOW()
			WHERE id = $1 AND published_at IS NULL
		`, event.ID)
		if err != nil {
			log.Println("Failed to mark event published:", err)
			return
		}

		log.Printf("Published OrderCreated event %s", event.ID)
	}
}
