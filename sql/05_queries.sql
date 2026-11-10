SELECT
    toYYYYMM(day) AS month,
    dictGet('shop.products_dict', 'category', product_id) AS category,
    sum(revenue) AS monthly_revenue
FROM shop.daily_sales
WHERE toYear(day) = 2025
GROUP BY month, category
ORDER BY month, monthly_revenue DESC;

SELECT
    dictGet('shop.products_dict', 'category', product_id) AS category,
    product_id,
    dictGet('shop.products_dict', 'title', product_id) AS title,
    sum(revenue) AS product_revenue
FROM shop.daily_sales
GROUP BY category, product_id
ORDER BY category, product_revenue DESC
LIMIT 3 BY category;

SELECT
    city,
    sumIf(quantity * unit_price, status = 'delivered') AS revenue,
    countIf(status = 'cancelled') AS cancelled_orders,
    round(countIf(status = 'returned') * 100.0 / count(), 2) AS returned_percentage
FROM shop.orders
GROUP BY city
ORDER BY revenue DESC;

SELECT
    customer_id,
    sum(quantity * unit_price) AS total_spent,
    argMax(order_id, quantity * unit_price) AS most_expensive_order_id,
    max(quantity * unit_price) AS max_order_amount
FROM shop.orders
WHERE status = 'delivered'
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

SELECT
    day,
    daily_rev,
    sum(daily_rev) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
    round(avg(daily_rev) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS moving_avg_7d
FROM (
    SELECT day, sum(revenue) AS daily_rev
    FROM shop.daily_sales
    GROUP BY day
)
ORDER BY day;

WITH monthly_city_sales AS (
    SELECT
        toYYYYMM(day) AS month,
        city,
        sum(revenue) AS rev
    FROM shop.daily_sales
    GROUP BY month, city
)
SELECT
    month,
    city,
    rev,
    rank() OVER (PARTITION BY month ORDER BY rev DESC) AS city_rank,
    lagInFrame(rev, 1, 0) OVER (PARTITION BY city ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prev_month_rev,
    if(prev_month_rev = 0, NULL, round((rev - prev_month_rev) * 100.0 / prev_month_rev, 2)) AS pct_change
FROM monthly_city_sales
ORDER BY month, city_rank;

SELECT
    first_month,
    count() AS new_customers
FROM (
    SELECT
        customer_id,
        min(toYYYYMM(created_at)) AS first_month
    FROM shop.orders
    GROUP BY customer_id
)
GROUP BY first_month
ORDER BY first_month;