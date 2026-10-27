package main

import (
	"encoding/json"
	"testing"
)

func TestProcessPayment(t *testing.T) {
	payload := []byte(`{
		"id": "11111111-1111-4111-8111-111111111111",
		"amount_cents": 2500,
		"status": "PENDING"
	}`)

	result, err := processPayment(payload)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result.OrderID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("unexpected order ID: %s", result.OrderID)
	}
	if result.AmountCents != 2500 {
		t.Fatalf("expected amount 2500, got %d", result.AmountCents)
	}
	if result.Status != "SUCCEEDED" {
		t.Fatalf("expected SUCCEEDED, got %s", result.Status)
	}
	if result.EventID == "" {
		t.Fatal("expected a non-empty event ID")
	}
}

func TestProcessPaymentRejectsInvalidOrder(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"invalid JSON", `{`},
		{"missing order ID", `{"amount_cents":2500}`},
		{"zero amount", `{"id":"11111111-1111-4111-8111-111111111111","amount_cents":0}`},
		{"negative amount", `{"id":"11111111-1111-4111-8111-111111111111","amount_cents":-1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := processPayment([]byte(tt.payload))
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestPaymentResultJSON(t *testing.T) {
	result := PaymentResultEvent{
		EventType:   "PaymentSucceeded",
		EventID:     "event-123",
		OrderID:     "order-123",
		AmountCents: 2500,
		Status:      "SUCCEEDED",
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded PaymentResultEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.OrderID != result.OrderID ||
		decoded.EventID != result.EventID ||
		decoded.Status != result.Status {
		t.Fatalf("round-trip mismatch: got %+v", decoded)
	}
}
