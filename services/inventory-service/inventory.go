package main

import (
	"context"
	"database/sql"
	"fmt"
)

func reserveStock(
	ctx context.Context,
	db *sql.DB,
	productID string,
	quantity int,
) error {
	if productID == "" || quantity <= 0 {
		return fmt.Errorf("invalid product ID or quantity")
	}

	result, err := db.ExecContext(ctx, `
		UPDATE inventory_stock
		SET quantity = quantity - $1,
		    updated_at = NOW()
		WHERE product_id = $2
		  AND quantity >= $1
	`, quantity, productID)
	if err != nil {
		return fmt.Errorf("update inventory: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check inventory update: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("insufficient stock or product not found")
	}

	return nil
}
