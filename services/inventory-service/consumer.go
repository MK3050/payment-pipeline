package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

func consumePaymentResults(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "payment-results",
		GroupID:  "inventory-service",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "inventory-results",
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	db, err := connectDB()
	if err != nil {
		log.Println("Inventory database connection failed:", err)
		return
	}
	defer db.Close()

	log.Println("Inventory Service consuming payment-results")

	for {
		message, err := reader.ReadMessage(ctx)
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

		err = reserveStock(ctx, db, "product-1", 1)

		result := InventoryResultEvent{
			EventID:   payment.EventID,
			OrderID:   payment.OrderID,
			EventType: "InventoryReserved",
			Status:    "SUCCEEDED",
		}

		if err != nil {
			result.EventType = "InventoryFailed"
			result.Status = "FAILED"
			result.Reason = err.Error()
		}

		payload, err := json.Marshal(result)
		if err != nil {
			log.Println("Failed to encode inventory result:", err)
			continue
		}

		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(payment.OrderID),
			Value: payload,
		})
		if err != nil {
			log.Println("Failed to publish inventory result:", err)
			continue
		}

		log.Printf("Order %s inventory result: %s", payment.OrderID, result.Status)
	}
}
