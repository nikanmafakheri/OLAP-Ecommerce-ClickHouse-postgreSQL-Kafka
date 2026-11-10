CREATE DATABASE IF NOT EXISTS shop;


CREATE TABLE shop.orders (
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
PARTITION BY toYYYYMM(created_at)
ORDER BY (city, status, toDate(created_at), product_id);


CREATE TABLE shop.orders_naive (
    order_id String,
    customer_id String,
    product_id String,
    city String,
    status String,
    quantity String,
    unit_price String,
    created_at String
)
ENGINE = MergeTree
ORDER BY tuple();