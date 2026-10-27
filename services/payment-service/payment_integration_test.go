package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestProcessOrderPaymentIsIdempotent(t *testing.T) {
	db, err := connectDB()
	if err != nil {
		t.Fatalf("database connection failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	orderID := uuid.New().String()

	payload, err := json.Marshal(OrderCreatedEvent{
		ID:          orderID,
		AmountCents: 2500,
		Status:      "PENDING",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Clean up only the records created by this test.
	defer db.ExecContext(ctx,
		`DELETE FROM payment_outbox WHERE aggregate_id = $1`, orderID)
	defer db.ExecContext(ctx,
		`DELETE FROM payments WHERE order_id = $1`, orderID)

	// Process the same order twice.
	for i := 0; i < 2; i++ {
		if err := processOrderPayment(ctx, db, payload); err != nil {
			t.Fatalf("processing attempt %d failed: %v", i+1, err)
		}
	}

	var paymentCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM payments WHERE order_id = $1
	`, orderID).Scan(&paymentCount)
	if err != nil {
		t.Fatal(err)
	}
	if paymentCount != 1 {
		t.Fatalf("expected 1 payment record, got %d", paymentCount)
	}

	var outboxCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM payment_outbox WHERE aggregate_id = $1
	`, orderID).Scan(&outboxCount)
	if err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected 1 payment outbox event, got %d", outboxCount)
	}
}
