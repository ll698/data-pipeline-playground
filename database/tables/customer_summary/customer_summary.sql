CREATE TABLE IF NOT EXISTS customer_summary (
 customer_id CHAR(256),
 total_order_expense REAL,
 last_order_date DATE,
 last_ship_address VARCHAR(60),
 last_ship_city VARCHAR(60),
 last_ship_postal_code VARCHAR(10),
 local_currency VARCHAR(24)
);