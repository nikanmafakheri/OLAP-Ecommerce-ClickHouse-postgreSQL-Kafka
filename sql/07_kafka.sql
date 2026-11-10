CREATE TABLE shop.orders_queue (
    order_id UInt64,
    customer_id UInt32,
    product_id UInt16,
    city LowCardinality(String),
    status Enum8('delivered' = 1, 'cancelled' = 2, 'returned' = 3),
    quantity UInt8,
    unit_price UInt32,
    created_at DateTime
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'orders',
    kafka_group_name = 'clickhouse_orders',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW shop.orders_kafka_mv TO shop.orders AS
SELECT
    order_id, customer_id, product_id, city, status, quantity, unit_price, created_at
FROM shop.orders_queue
WHERE length(_error) = 0;

CREATE TABLE shop.orders_queue_errors (
    _partition UInt64,
    _offset UInt64,
    _raw_message String,
    _error String
)
ENGINE = MergeTree
ORDER BY (_partition, _offset);

CREATE MATERIALIZED VIEW shop.orders_kafka_errors_mv TO shop.orders_queue_errors AS
SELECT
    _partition,
    _offset,
    _raw_message,
    _error
FROM shop.orders_queue
WHERE length(_error) > 0;