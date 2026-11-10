CREATE TABLE shop.daily_sales (
    day Date,
    city LowCardinality(String),
    product_id UInt16,
    orders UInt64,
    items UInt64,
    revenue UInt64
)
ENGINE = SummingMergeTree
ORDER BY (day, city, product_id);

CREATE MATERIALIZED VIEW shop.daily_sales_mv TO shop.daily_sales AS
SELECT
    toDate(created_at) AS day,
    city,
    product_id,
    count() AS orders,
    sum(quantity) AS items,
    sum(quantity * unit_price) AS revenue
FROM shop.orders
WHERE status = 'delivered'
GROUP BY day, city, product_id;

INSERT INTO shop.daily_sales
SELECT
    toDate(created_at) AS day,
    city,
    product_id,
    count() AS orders,
    sum(quantity) AS items,
    sum(quantity * unit_price) AS revenue
FROM shop.orders
WHERE status = 'delivered'
GROUP BY day, city, product_id;


CREATE TABLE shop.daily_customers (
    day Date,
    unique_customers AggregateFunction(uniq, UInt32),
    avg_order_amount AggregateFunction(avg, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY day;

CREATE MATERIALIZED VIEW shop.daily_customers_mv TO shop.daily_customers AS
SELECT
    toDate(created_at) AS day,
    uniqState(customer_id) AS unique_customers,
    avgState(toUInt64(quantity * unit_price)) AS avg_order_amount
FROM shop.orders
GROUP BY day;

INSERT INTO shop.daily_customers
SELECT
    toDate(created_at) AS day,
    uniqState(customer_id) AS unique_customers,
    avgState(toUInt64(quantity * unit_price)) AS avg_order_amount
FROM shop.orders
GROUP BY day;


CREATE TABLE shop.top_products (
    product_id UInt16,
    total_revenue UInt64,
    total_items UInt64
)
ENGINE = MergeTree
ORDER BY total_revenue;

CREATE MATERIALIZED VIEW shop.top_products_mv
REFRESH EVERY 10 MINUTE
TO shop.top_products
AS
SELECT
    product_id,
    sum(revenue) AS total_revenue,
    sum(items) AS total_items
FROM shop.daily_sales
WHERE day >= (SELECT max(day) - INTERVAL 30 DAY FROM shop.daily_sales)
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10;

SELECT view, status, last_success_time, next_refresh_time
FROM system.view_refreshes
WHERE database = 'shop';


CREATE TABLE shop.pg_products (
    product_id Int32,
    title String,
    category String,
    brand String
)
ENGINE = PostgreSQL('postgres:5432', 'shopland', 'products', 'postgres', 'password');

CREATE DICTIONARY shop.products_dict (
    product_id UInt16,
    title String,
    category String,
    brand String
)
PRIMARY KEY product_id
SOURCE(POSTGRESQL(
    port 5432
    host 'postgres'
    user 'postgres'
    password 'password'
    db 'shopland'
    table 'products'
))
LIFETIME(MIN 300 MAX 600)
LAYOUT(FLAT());

SELECT dictGet('shop.products_dict', 'category', toUInt16(10)) AS product_10_category;


SELECT
    toYYYYMM(day) AS month,
    uniqMerge(unique_customers) AS approx_unique_customers
FROM shop.daily_customers
GROUP BY month
ORDER BY month;

SELECT
    toYYYYMM(created_at) AS month,
    uniqExact(customer_id) AS exact_unique_customers
FROM shop.orders
GROUP BY month
ORDER BY month;

SELECT p.category, sum(o.quantity * o.unit_price) AS revenue
FROM shop.orders AS o
JOIN shop.pg_products AS p ON o.product_id = p.product_id
WHERE o.status = 'delivered'
GROUP BY p.category;

SELECT dictGet('shop.products_dict', 'category', product_id) AS category, sum(quantity * unit_price) AS revenue
FROM shop.orders
WHERE status = 'delivered'
GROUP BY category;