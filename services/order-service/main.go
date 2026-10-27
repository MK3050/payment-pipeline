package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

type CreateOrderRequest struct {
	AmountCents int64 `json:"amount_cents"`
}

type Order struct {
	ID          string `json:"id"`
	AmountCents int64  `json:"amount_cents"`
	Status      string `json:"status"`
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found; using system environment variables")
	}
	db, err := connectDB()
	if err != nil {
		log.Fatal("Database connection failed: ", err)
	}
	defer db.Close()
	go runOutboxPublisher(context.Background(), db)
	go consumePaymentResults(context.Background(), db)
	go consumeInventoryResults(context.Background(), db)
	go consumeRefundResults(context.Background(), db)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(); err != nil {
			http.Error(w, "Database unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "Order Service is running; database is connected")
	})

	http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req CreateOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if req.AmountCents <= 0 {
			http.Error(w, "amount_cents must be positive", http.StatusBadRequest)
			return
		}

		id := uuid.New().String()

		tx, err := db.Begin()
		if err != nil {
			log.Println("Failed to begin transaction:", err)
			http.Error(w, "Failed to create order", http.StatusInternalServerError)
			return
		}
		defer tx.Rollback()

		_, err = tx.Exec(
			`INSERT INTO orders (id, amount_cents, status)
	 VALUES ($1, $2, $3)`,
			id, req.AmountCents, "PENDING",
		)
		if err != nil {
			log.Println("Failed to insert order:", err)
			http.Error(w, "Failed to create order", http.StatusInternalServerError)
			return
		}

		eventID := uuid.New().String()

		payload, err := json.Marshal(Order{
			ID:          id,
			AmountCents: req.AmountCents,
			Status:      "PENDING",
		})
		if err != nil {
			log.Println("Failed to encode event:", err)
			http.Error(w, "Failed to create order", http.StatusInternalServerError)
			return
		}

		_, err = tx.Exec(
			`INSERT INTO outbox_events
	 (id, aggregate_id, event_type, payload)
	 VALUES ($1, $2, $3, $4)`,
			eventID, id, "OrderCreated", payload,
		)
		if err != nil {
			log.Println("Failed to insert outbox event:", err)
			http.Error(w, "Failed to create order", http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(); err != nil {
			log.Println("Failed to commit transaction:", err)
			http.Error(w, "Failed to create order", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(Order{
			ID:          id,
			AmountCents: req.AmountCents,
			Status:      "PENDING",
		})
	})

	log.Println("Order Service listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
