SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders
WHERE customer_id = 4321
ORDER BY created_at DESC;

SYSTEM FLUSH LOGS;

SELECT
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    formatReadableSize(memory_usage) AS memory_usage
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '%WHERE customer_id = 4321%'
  AND query NOT LIKE '%system.query_log%'
ORDER BY event_time DESC
LIMIT 1;

EXPLAIN indexes = 1
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders
WHERE customer_id = 4321
ORDER BY created_at DESC;

CREATE TABLE shop.orders_by_customer (
    order_id UInt64 CODEC(Delta, ZSTD(1)),
    customer_id UInt32 CODEC(ZSTD(1)),
    product_id UInt16 CODEC(ZSTD(1)),
    city LowCardinality(String) CODEC(ZSTD(1)),
    status Enum8('delivered' = 1, 'cancelled' = 2, 'returned' = 3) CODEC(ZSTD(1)),
    quantity UInt8 CODEC(ZSTD(1)),
    unit_price UInt32 CODEC(ZSTD(1)),
    created_at DateTime CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (customer_id, created_at);

CREATE MATERIALIZED VIEW shop.orders_by_customer_mv TO shop.orders_by_customer AS
SELECT * FROM shop.orders;

INSERT INTO shop.orders_by_customer SELECT * FROM shop.orders;

SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders_by_customer
WHERE customer_id = 4321
ORDER BY created_at DESC;

EXPLAIN indexes = 1
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders_by_customer
WHERE customer_id = 4321
ORDER BY created_at DESC;

SYSTEM FLUSH LOGS;

SELECT
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    formatReadableSize(memory_usage) AS memory_usage
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '%FROM shop.orders_by_customer%'
  AND query NOT LIKE '%system.query_log%'
  AND query NOT LIKE '%EXPLAIN%'
ORDER BY event_time DESC
LIMIT 1;

SELECT
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes_readable,
    substring(query, 1, 100) AS query_snippet
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query NOT LIKE '%system.%'
ORDER BY read_bytes DESC
LIMIT 3;