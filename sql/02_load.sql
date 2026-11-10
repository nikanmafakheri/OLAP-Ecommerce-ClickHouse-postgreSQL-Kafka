INSERT INTO shop.orders
SELECT
    number + 1 AS order_id,
    rand(1) % 200000 + 1 AS customer_id,
    rand(2) % 5000 + 1 AS product_id,
    ['Tehran', 'Tehran', 'Tehran', 'Mashhad', 'Isfahan', 'Shiraz', 'Tabriz', 'Karaj'][rand(3) % 8 + 1] AS city,
    ['delivered', 'delivered', 'delivered', 'delivered', 'cancelled', 'returned'][rand(4) % 6 + 1] AS status,
    rand(5) % 5 + 1 AS quantity,
    (rand(6) % 2000 + 10) * 1000 AS unit_price,
    toDateTime('2025-01-01 00:00:00') + rand(7) % (365 * 86400) AS created_at
FROM numbers(10000000);


INSERT INTO shop.orders_naive
SELECT * FROM shop.orders;


OPTIMIZE TABLE shop.orders FINAL;
OPTIMIZE TABLE shop.orders_naive FINAL;

SELECT
    table,
    name AS column_name,
    type,
    formatReadableSize(data_compressed_bytes) AS compressed,
    formatReadableSize(data_uncompressed_bytes) AS uncompressed,
    round(data_uncompressed_bytes / data_compressed_bytes, 2) AS ratio
FROM system.columns
WHERE database = 'shop' AND table IN ('orders', 'orders_naive')
ORDER BY table, name;

SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS total_compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS total_uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS total_ratio
FROM system.columns
WHERE database = 'shop' AND table IN ('orders', 'orders_naive')
GROUP BY table;

INSERT INTO FUNCTION file('orders_202503.parquet', 'Parquet')
SELECT * FROM shop.orders WHERE toYYYYMM(created_at) = 202503;

INSERT INTO FUNCTION file('orders_202503.csv', 'CSVWithNames')
SELECT * FROM shop.orders WHERE toYYYYMM(created_at) = 202503;

SELECT count() AS table_count FROM shop.orders WHERE toYYYYMM(created_at) = 202503;
SELECT count() AS parquet_count FROM file('orders_202503.parquet', 'Parquet');
DESCRIBE file('orders_202503.parquet', 'Parquet');

CREATE TABLE shop.small_inserts (
    id UInt32,
    ts DateTime
) ENGINE = MergeTree ORDER BY id;