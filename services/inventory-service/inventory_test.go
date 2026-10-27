package main

import (
	"context"
	"testing"
)

func TestReserveStock(t *testing.T) {
	db, err := connectDB()
	if err != nil {
		t.Fatalf("database connection failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	productID := "test-product-stock"

	_, err = db.ExecContext(ctx, `
		INSERT INTO inventory_stock (product_id, quantity)
		VALUES ($1, 5)
		ON CONFLICT (product_id)
		DO UPDATE SET quantity = 5, updated_at = NOW()
	`, productID)
	if err != nil {
		t.Fatalf("failed to prepare stock: %v", err)
	}
	defer db.ExecContext(ctx,
		`DELETE FROM inventory_stock WHERE product_id = $1`, productID)

	if err := reserveStock(ctx, db, productID, 2); err != nil {
		t.Fatalf("expected reservation to succeed: %v", err)
	}

	var quantity int
	err = db.QueryRowContext(ctx,
		`SELECT quantity FROM inventory_stock WHERE product_id = $1`,
		productID,
	).Scan(&quantity)
	if err != nil {
		t.Fatalf("failed to query stock: %v", err)
	}
	if quantity != 3 {
		t.Fatalf("expected quantity 3, got %d", quantity)
	}
}

func TestReserveStockRejectsInsufficientStock(t *testing.T) {
	db, err := connectDB()
	if err != nil {
		t.Fatalf("database connection failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	productID := "test-product-insufficient"

	_, err = db.ExecContext(ctx, `
		INSERT INTO inventory_stock (product_id, quantity)
		VALUES ($1, 1)
		ON CONFLICT (product_id)
		DO UPDATE SET quantity = 1, updated_at = NOW()
	`, productID)
	if err != nil {
		t.Fatalf("failed to prepare stock: %v", err)
	}
	defer db.ExecContext(ctx,
		`DELETE FROM inventory_stock WHERE product_id = $1`, productID)

	err = reserveStock(ctx, db, productID, 2)
	if err == nil {
		t.Fatal("expected reservation to fail when stock is insufficient")
	}

	var quantity int
	err = db.QueryRowContext(ctx,
		`SELECT quantity FROM inventory_stock WHERE product_id = $1`,
		productID,
	).Scan(&quantity)
	if err != nil {
		t.Fatalf("failed to query stock: %v", err)
	}
	if quantity != 1 {
		t.Fatalf("expected quantity to remain 1, got %d", quantity)
	}
}
