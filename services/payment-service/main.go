package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found; using environment variables")
	}

	db, err := connectDB()
	if err != nil {
		log.Fatal("Database connection failed:", err)
	}
	defer db.Close()

	ctx := context.Background()

	go runPaymentOutboxPublisher(ctx, db)
	go consumeOrders(ctx, db)
	go consumeRefundRequests(ctx)
	go runRefundOutboxPublisher(ctx, db)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := db.PingContext(r.Context()); err != nil {
			http.Error(w, "Database unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "Payment Service is running")
	})

	log.Println("Payment Service listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
