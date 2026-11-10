CREATE TABLE shop.order_status_events (
    order_id UInt64,
    status LowCardinality(String),
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_id;

INSERT INTO shop.order_status_events
SELECT order_id, 'paid', created_at FROM shop.orders WHERE order_id % 20 = 0;

INSERT INTO shop.order_status_events
SELECT order_id, 'shipped', created_at + 3600 FROM shop.orders WHERE order_id % 40 = 0;

INSERT INTO shop.order_status_events
SELECT order_id, 'delivered', created_at + 86400 FROM shop.orders WHERE order_id % 80 = 0;

SELECT count() AS count_without_final FROM shop.order_status_events;
SELECT count() AS count_with_final FROM shop.order_status_events FINAL;

SELECT status, count() AS cnt
FROM shop.order_status_events FINAL
GROUP BY status;

SELECT final_status, count() AS cnt
FROM (
    SELECT order_id, argMax(status, updated_at) AS final_status
    FROM shop.order_status_events
    GROUP BY order_id
)
GROUP BY final_status;


ALTER TABLE shop.orders
UPDATE status = 'cancelled'
WHERE customer_id = 777 AND toYYYYMM(created_at) = 202503
SETTINGS mutations_sync = 1;

SELECT mutation_id, command, parts_to_do, is_done
FROM system.mutations
WHERE database = 'shop' AND table = 'orders';

DELETE FROM shop.orders WHERE customer_id = 12345;


CREATE TABLE shop.ttl_demo (
    id UInt32,
    ts DateTime
)
ENGINE = MergeTree
ORDER BY id
TTL ts + INTERVAL 1 MINUTE;

INSERT INTO shop.ttl_demo SELECT number, now() - 120 FROM numbers(10);
INSERT INTO shop.ttl_demo SELECT number + 10, now() FROM numbers(5);

SELECT count() AS before_optimize FROM shop.ttl_demo;

OPTIMIZE TABLE shop.ttl_demo FINAL;

SELECT count() AS after_optimize FROM shop.ttl_demo;