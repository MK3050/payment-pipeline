package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
)

func main() {
	go consumeOrders(context.Background())
	go consumeRefundRequests(context.Background())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Payment Service is running")
	})

	log.Println("Payment Service listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
