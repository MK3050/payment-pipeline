package main

type PaymentResultEvent struct {
	EventID     string `json:"event_id"`
	EventType   string `json:"event_type"`
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
	Status      string `json:"status"`
}

type OrderItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type InventoryResultEvent struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
}
