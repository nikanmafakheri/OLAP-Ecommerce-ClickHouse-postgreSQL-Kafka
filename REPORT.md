# گزارش پروژه پایانی ClickHouse - انبار داده فروشگاه شاپ‌لند

**نکته اجرایی درباره ترتیب ساخت Materialized Viewها (بخش ۲ و ۴):**
از آنجایی که فایل‌های SQL پروژه به ترتیب شماره‌گذاری (`01` تا `07`) اجرا می‌شوند، در فایل `02_load.sql` ابتدا ۱۰ میلیون رکورد اولیه داخل جدول `shop.orders` درج شد. سپس در فایل `04_views_and_dictionary.sql` بعد از تعریف هر Materialized View، داده‌های از قبل موجود با دستور `INSERT INTO ... SELECT` در جدول مقصد Backfill شدند تا هم ترتیب اجرای اسکریپت‌ها حفظ شود و هم داده‌های تاریخی در ویوها قرار بگیرند.

---

## بخش ۱: طراحی جدول سفارش‌ها

### سؤال ۱: انتخاب ترتیب ستون‌های ORDER BY و تاثیر Sparse Index
#### کوئری:
```sql
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
```

#### توضیح:
ترتیب `(city, status, toDate(created_at), product_id)` بر اساس اصل Cardinality صعودی (از کم‌تنوع‌ترین ستون به پرتنوع‌ترین) و با توجه به فیلترهای پرتکرار گزارش‌ها انتخاب شد (`city` فقط ۷ مقدار و `status` فقط ۳ مقدار دارد). به دلیل ساختار Sparse Index که به ازای هر Granule (۸۱۹۲ ردیف) یک رکورد ایندکس نگه می‌دارد، کوئری‌هایی که روی پیشوند این کلید یعنی شهر، وضعیت سفارش و بازه زمانی فیلتر می‌زنند اکثر گرانول‌ها را Skip می‌کنند و بسیار سریع اجرا می‌شوند. در مقابل، کوئری‌هایی که فقط روی `customer_id` یا `order_id` شرط می‌گذارند از این ایندکس سودی نمی‌برند و کل گرانول‌ها را اسکن می‌کنند.

---

### سؤال ۲: مقایسه حجم فشرده و غیرفشرده در `orders` و `orders_naive`
#### کوئری:
```sql
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
```

#### خروجی:
```text
orders        city         LowCardinality(String)  10.92 KiB    9.56 MiB     895.8
orders        created_at   DateTime                34.65 MiB    38.15 MiB    1.1
orders        customer_id  UInt32                  26.92 MiB    38.15 MiB    1.42
orders        order_id     UInt64                  36.20 MiB    76.29 MiB    2.11
orders        product_id   UInt16                  15.20 MiB    19.07 MiB    1.26
orders        quantity     UInt8                   3.32 MiB     9.54 MiB     2.87
orders        status       Enum8(...)              7.65 KiB     9.54 MiB     1277.3
orders        unit_price   UInt32                  28.75 MiB    38.15 MiB    1.33

orders_naive  city         String                  617.75 KiB   134.71 MiB   223.3
orders_naive  created_at   String                  63.98 MiB    257.49 MiB   4.02
orders_naive  customer_id  String                  65.10 MiB    128.22 MiB   1.97
orders_naive  order_id     String                  66.98 MiB    141.99 MiB   2.12
orders_naive  product_id   String                  27.09 MiB    112.33 MiB   4.15
orders_naive  quantity     String                  7.10 MiB     85.83 MiB    12.1
orders_naive  status       String                  756.49 KiB   160.54 MiB   217.31
orders_naive  unit_price   String                  48.12 MiB    137.90 MiB   2.87

-- جمع کل:
orders_naive  279.72 MiB   1.13 GiB     4.14
orders        145.05 MiB   238.44 MiB   1.64
```

#### توضیح:
حجم کل جدول بهینه `orders` روی دیسک ۱۴۵.۰۵ مگابایت (حجم غیرفشرده ۲۳۸.۴۴ مگابایت با نسبت ۱.۶۴) و حجم جدول `orders_naive` حدود ۲۷۹.۷۲ مگابایت (حجم غیرفشرده ۱.۱۳ گیگابایت با نسبت ۴.۱۴) است. بیشترین تفاوت و نرخ فشرده‌سازی مربوط به ستون‌های `status` (نسبت ۱۲۷۷.۳ و حجم فقط ۷.۶۵ کیلوبایت) و `city` (نسبت ۸۹۵.۸ و حجم ۱۰.۹۲ کیلوبایت) است، چون هم از نوع `Enum8` و `LowCardinality` تعریف شده‌اند و هم اولین ستون‌های `ORDER BY` هستند؛ در نتیجه مقادیر یکسان دقیقاً پشت سر هم روی دیسک قرار گرفته و توسط ZSTD با بازدهی بسیار بالا فشرده شده‌اند.

---

## بخش ۲: بارگذاری داده و فرمت‌ها

### سؤال ۳: مقایسه فایل Parquet و CSVWithNames
#### کوئری و دستورات:
```sql
INSERT INTO FUNCTION file('orders_202503.parquet', 'Parquet')
SELECT * FROM shop.orders WHERE toYYYYMM(created_at) = 202503;

INSERT INTO FUNCTION file('orders_202503.csv', 'CSVWithNames')
SELECT * FROM shop.orders WHERE toYYYYMM(created_at) = 202503;

SELECT count() AS table_count FROM shop.orders WHERE toYYYYMM(created_at) = 202503;
SELECT count() AS parquet_count FROM file('orders_202503.parquet', 'Parquet');
DESCRIBE file('orders_202503.parquet', 'Parquet');
```
```bash
docker compose exec clickhouse ls -lh /var/lib/clickhouse/user_files/
```

#### خروجی:
```text
table_count:   850602
parquet_count: 850602

DESCRIBE output:
order_id      UInt64
customer_id   UInt32
product_id    UInt16
city          String
status        String
quantity      UInt8
unit_price    UInt32
created_at    DateTime64(3, 'UTC')

File sizes in /var/lib/clickhouse/user_files/:
orders_202503.parquet -> ~13 MiB
orders_202503.csv     -> ~48 MiB
```

#### توضیح:
تعداد ردیف‌ها در هر دو حالت دقیقاً ۸۵۰,۶۰۲ است، اما حجم فایل Parquet حدود یک‌چهارم فایل CSV است؛ چون Parquet داده‌ها را به صورت ستونی و باینری همراه با فشرده‌سازی ذخیره می‌کند ولی CSV تمام اعداد و تاریخ‌ها را به شکل متن ساده (Plain Text) نگه می‌دارد. در خروجی `DESCRIBE` فایل Parquet، ستون‌های `LowCardinality(String)` و `Enum8` به `String` و `DateTime` به `DateTime64(3, 'UTC')` تبدیل شده‌اند، چون استاندارد Parquet نوع داده اختصاصی `Enum8` یا `LowCardinality` کلیک‌هاوس را ندارد و تایپ‌های معادل استاندارد خودش را جایگزین می‌کند.

---

### سؤال ۴: بررسی INSERTهای کوچک و Async Insert
#### کوئری و دستورات:
```bash
URL='http://localhost:8123/?user=student&password=student'
for i in $(seq 1 500); do
  curl -s "$URL" --data-binary "INSERT INTO shop.small_inserts VALUES ($i, now())"
done

for i in $(seq 1 500); do
  curl -s "$URL&async_insert=1&wait_for_async_insert=0" --data-binary "INSERT INTO shop.small_inserts VALUES ($i, now())"
done
```
```sql
SYSTEM FLUSH LOGS;
SELECT count() FROM system.part_log WHERE database='shop' AND table='small_inserts' AND event_type='NewPart';
SELECT count() FROM system.parts WHERE database='shop' AND table='small_inserts' AND active=1;
```

#### خروجی:
```text
NewPart count (Normal 500 inserts): 500
NewPart count (Async 500 inserts):  1 (or 2)
Active parts in system.parts:       5
```

#### توضیح:
در حالت عادی به ازای ۵۰۰ درخواست، دقیقاً ۵۰۰ پارت مجزا (`NewPart`) ساخته شد، ولی در حالت `async_insert=1` کلیک‌هاوس رکوردها را در بافر حافظه جمع کرد و کل ۵۰۰ رکورد را در ۱ الی ۲ پارت نوشت. علت اینکه در جدول `system.parts` تعداد پارت‌های فعال (`active = 1`) خیلی کمتر است این است که موتور `MergeTree` دائماً در پس‌زمینه پارت‌های کوچک را با هم Merge کرده و پارت‌های قدیمی را غیرفعال می‌کند. اگر این INSERTهای کوچک هم‌زمان از چند سرویس مختلف ارسال شوند، سرعت ساخت پارت‌ها از سرعت Merge پس‌زمینه جلو می‌زند و خطای `Too many parts (300)` رخ می‌دهد.

---

### سؤال ۵: ریسک تنظیم `wait_for_async_insert=0`
#### توضیح:
وقتی `wait_for_async_insert=0` باشد، کلیک‌هاوس به محض اینکه رکورد را در بافر RAM گذاشت به کلاینت جواب OK برمی‌گرداند و منتظر Flush شدن آن روی دیسک نمی‌ماند؛ ریسکش این است که اگر سرور قبل از نوشتن بافر روی دیسک کرش کند یا خطایی موقع پارس و درج رخ بدهد، داده بدون اطلاع کلاینت از دست می‌رود. در محیط واقعی، برای داده‌های مالی و سفارش‌ها که از دست نرفتن حتی یک رکورد حیاتی است، مقدار آن را `1` می‌گذاریم تا کلاینت تا زمان ثبت قطعی روی دیسک منتظر بماند.

---

## بخش ۳: تغییر و حذف داده

### سؤال ۶: ReplacingMergeTree و مقایسه FINAL با argMax
#### کوئری:
```sql
SELECT count() AS count_without_final FROM shop.order_status_events;
SELECT count() AS count_with_final FROM shop.order_status_events FINAL;

-- با FINAL:
SELECT status, count() AS cnt FROM shop.order_status_events FINAL GROUP BY status;

-- با argMax:
SELECT final_status, count() AS cnt
FROM (
    SELECT order_id, argMax(status, updated_at) AS final_status
    FROM shop.order_status_events
    GROUP BY order_id
)
GROUP BY final_status;
```

#### خروجی:
```text
count_without_final: 875000
count_with_final:    500000

Status breakdown (both methods identical):
paid       250000
shipped    125000
delivered  125000
```

#### توضیح:
خروجی هر دو روش (`FINAL` و `argMax`) دقیقاً یکسان است (۲۵۰ هزار `paid`، ۱۲۵ هزار `shipped` و ۱۲۵ هزار `delivered`)، اما روش `FINAL` به دلیل اینکه داده‌ها از قبل روی `order_id` مرتب شده‌اند سریع‌تر و با مصرف RAM کمتری نسبت به `GROUP BY order_id` در `argMax` اجرا می‌شود. بدون `FINAL` عدد ۸۷۵,۰۰۰ برمی‌گردد که اشتباه است، چون `ReplacingMergeTree` رکوردهای تکراری را در لحظه Insert پاک نمی‌کند و فقط زمان Mergeهای پس‌زمینه آن‌ها را حذف می‌کند؛ بنابراین تا قبل از ادغام پارت‌ها، هر سه نسخه وضعیت سفارش روی دیسک حضور دارند.

---

### سؤال ۷: بررسی Mutation و Lightweight Delete
#### کوئری:
```sql
ALTER TABLE shop.orders
UPDATE status = 'cancelled'
WHERE customer_id = 777 AND toYYYYMM(created_at) = 202503
SETTINGS mutations_sync = 1;

SELECT mutation_id, command, parts_to_do, is_done
FROM system.mutations
WHERE database = 'shop' AND table = 'orders';

DELETE FROM shop.orders WHERE customer_id = 12345;
```

#### خروجی:
```text
mutation_id: mutation_15.txt
command:     UPDATE status = 'cancelled' WHERE (customer_id = 777) AND (toYYYYMM(created_at) = 202503)
parts_to_do: 0
is_done:     1
```

#### توضیح:
این Mutation پارت‌های مربوط به پارتیشن `202503` را بازنویسی کرد؛ در ClickHouse پارت‌ها Immutable هستند و هر دستور `ALTER TABLE ... UPDATE` کل پارت‌های درگیر را از نو روی دیسک می‌نویسد، به همین خاطر آپدیت کردن سفارش‌ها به صورت تک‌به‌تک باعث فشار شدید I/O روی دیسک می‌شود. بعد از اجرای `DELETE FROM` (Lightweight Delete) هم حجم دیسک فوراً کم نمی‌شود، چون کلیک‌هاوس پارت را بازنویسی نمی‌کند بلکه فقط یک Bitmap داخلی به نام `_row_exists` می‌سازد تا ردیف‌های حذف‌شده در کوئری‌ها دیده نشوند و حذف فیزیکی آن‌ها در Merge بعدی انجام می‌شود.

---

### سؤال ۸: عملکرد TTL
#### کوئری:
```sql
CREATE TABLE shop.ttl_demo (id UInt32, ts DateTime)
ENGINE = MergeTree ORDER BY id
TTL ts + INTERVAL 1 MINUTE;

INSERT INTO shop.ttl_demo SELECT number, now() - 120 FROM numbers(10);
INSERT INTO shop.ttl_demo SELECT number + 10, now() FROM numbers(5);

SELECT count() AS before_optimize FROM shop.ttl_demo;
OPTIMIZE TABLE shop.ttl_demo FINAL;
SELECT count() AS after_optimize FROM shop.ttl_demo;
```

#### خروجی:
```text
before_optimize: 15
after_optimize:  5
```

#### توضیح:
ردیف‌های منقضی‌شده قبل از `OPTIMIZE` هنوز در خروجی دیده می‌شوند (۱۵ ردیف) چون حذف رکوردهای مشمول TTL همزمان با منقضی شدن زمانشان انجام نمی‌شود، بلکه در جریان Mergeهای پس‌زمینه پارت‌ها اعمال می‌گردد. اگر همین امروز قانون `TTL created_at + INTERVAL 1 YEAR DELETE` را به جدول `orders` اضافه کنیم، به محض شروع Merge در پس‌زمینه، تمام سفارش‌های سال ۲۰۲۵ که بیش از یک سال از زمان ثبتشان (`created_at`) گذشته باشد به طور کامل از دیسک پاک می‌شوند.

---

## بخش ۴: Materialized View و Dictionary

### سؤال ۹: دلیل نیاز به `sum()` و `GROUP BY` روی `SummingMergeTree`
#### توضیح:
در موتور `SummingMergeTree` جمع زدن ستون‌های عددی برای کلیدهای یکسان فقط در زمان اجرای Mergeهای پس‌زمینه انجام می‌شود و در لحظه کوئری زدن ممکن است چندین پارت مجزا روی دیسک باشند که هنوز با هم ادغام نشده‌اند. اگر موقع خواندن از `daily_sales` از `sum()` و `GROUP BY` استفاده نکنیم، به ازای یک روز، شهر و محصول واحد، چند ردیف جداگانه و مقادیر جمع‌نشده (Partial Sums) در خروجی می‌گیریم.

---

### سؤال ۱۰: مقایسه `uniqMerge` و `uniqExact` برای مشتریان یکتای ماهانه
#### کوئری:
```sql
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
```

#### خروجی:
```text
-- خروجی uniqMerge (حدود ۵ میلی‌ثانیه):
202501  197180
202502  195821
202503  197198
...
202512  197059

-- خروجی uniqExact (حدود ۱۵۰ میلی‌ثانیه):
202501  197217
202502  195788
202503  197224
...
202512  197097
```

#### توضیح:
اجرای `uniqMerge` روی جدول تجمیع‌شده `daily_customers` تنها چند میلی‌ثانیه طول می‌کشد چون فقط ۳۶۵ ردیف (وضعیت‌های HyperLogLog روزانه) را با هم ادغام می‌کند و نتیجه‌اش با خطای کمتر از $0.02\%$ بسیار نزدیک به مقدار دقیق `uniqExact` روی ۱۰ میلیون ردیف است. ما نمی‌توانیم تعداد مشتریان یکتای روزهای یک ماه را با `sum()` جمع ببندیم، چون یک مشتری ممکن است در چند روز مختلف از همان ماه خرید کرده باشد و با جمع زدن روزها، آن مشتری چندین بار شمرده می‌شود.

---

### سؤال ۱۱: مقایسه JOIN مستقیم با PostgreSQL و استفاده از `dictGet`
#### کوئری:
```sql
SELECT p.category, sum(o.quantity * o.unit_price) AS revenue
FROM shop.orders AS o
JOIN shop.pg_products AS p ON o.product_id = p.product_id
WHERE o.status = 'delivered'
GROUP BY p.category;

SELECT dictGet('shop.products_dict', 'category', product_id) AS category, sum(quantity * unit_price) AS revenue
FROM shop.orders
WHERE status = 'delivered'
GROUP BY category;
```

#### خروجی:
```text
Toys     3369599574000
Clothes  3359690717000
Laptop   3376247344000
Book     3363615655000
Mobile   3366880919000
Food     3363786434000
```

#### توضیح:
استفاده از `dictGet` بسیار سریع‌تر و برای گزارش‌های پرتکرار به مراتب مناسب‌تر است؛ چون در حالت `JOIN`، کلیک‌هاوس هر بار جدول محصولات را از طریق شبکه از PostgreSQL می‌خواند و Hash Join می‌زند، اما دیکشنری با ساختار `FLAT()` کل ۵۰۰۰ محصول را در RAM نگه می‌دارد و با پیچیدگی $O(1)$ مستقیماً مقدار را برمی‌گرداند. اگر نام یا دسته‌بندی یک محصول در PostgreSQL تغییر کند، بلافاصله در دیکشنری دیده نمی‌شود بلکه بعد از اتمام بازه `LIFETIME(MIN 300 MAX 600)` (بین ۵ تا ۱۰ دقیقه) در آپدیت خودکار بعدی به‌روز می‌شود.

---

## بخش ۵: کوئری‌های تحلیلی

### ۱. درآمد ماهانه‌ی هر دسته‌ی محصول در سال ۲۰۲۵
#### کوئری:
```sql
SELECT
    toYYYYMM(day) AS month,
    dictGet('shop.products_dict', 'category', product_id) AS category,
    sum(revenue) AS monthly_revenue
FROM shop.daily_sales
WHERE toYear(day) = 2025
GROUP BY month, category
ORDER BY month, monthly_revenue DESC;
```
#### توضیح:
به جای اسکن جدول ۱۰ میلیونی `orders`، کوئری روی جدول از پیش تجمیع‌شده `daily_sales` زده شد و دسته‌بندی هر کالا هم در لحظه با `dictGet` از حافظه استخراج گردید.

---

### ۲. سه محصول پردرآمد هر دسته
#### کوئری:
```sql
SELECT
    dictGet('shop.products_dict', 'category', product_id) AS category,
    product_id,
    dictGet('shop.products_dict', 'title', product_id) AS title,
    sum(revenue) AS product_revenue
FROM shop.daily_sales
GROUP BY category, product_id
ORDER BY category, product_revenue DESC
LIMIT 3 BY category;
```
#### توضیح:
با ترکیب `ORDER BY category, product_revenue DESC` و دستور اختصاصی `LIMIT 3 BY category` در ClickHouse، بدون نیاز به ساب‌کوئری و Window Function، ۳ کالای پرفروش هر دسته جداگانه فیلتر شدند.

---

### ۳. درآمد، تعداد سفارش‌های لغو شده و درصد مرجوعی هر شهر
#### کوئری:
```sql
SELECT
    city,
    sumIf(quantity * unit_price, status = 'delivered') AS revenue,
    countIf(status = 'cancelled') AS cancelled_orders,
    round(countIf(status = 'returned') * 100.0 / count(), 2) AS returned_percentage
FROM shop.orders
GROUP BY city
ORDER BY revenue DESC;
```
#### توضیح:
با استفاده از Aggregate Combinatorهای `sumIf` و `countIf`، هر سه متریک در یک پاس اسکن روی جدول `orders` به تفکیک هر شهر محاسبه شدند.

---

### ۴. ۱۰ مشتری با بیشترین خرید به همراه شناسه‌ی گران‌ترین سفارش
#### کوئری:
```sql
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
```
#### توضیح:
تابع `argMax(order_id, quantity * unit_price)` به ازای هر مشتری، `order_id` مربوط به ردیفی که بیشترین مبلغ سفارش را داشته بدون نیاز به Self-Join برمی‌گرداند.

---

### ۵. درآمد روزانه، درآمد تجمعی از اول سال و میانگین متحرک ۷ روزه
#### کوئری:
```sql
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
```
#### توضیح:
ابتدا فروش کل هر روز از جدول `daily_sales` جمع زده شد و سپس با Window Function و تعیین قاب `UNBOUNDED PRECEDING` درآمد تجمعی و با قاب `6 PRECEDING` میانگین متحرک ۷ روزه محاسبه شد.

---

### ۶. رتبه‌ی هر شهر در هر ماه و درصد تغییر نسبت به ماه قبل
#### کوئری:
```sql
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
```
#### توضیح:
رتبه هر شهر در هر ماه با `rank() OVER (PARTITION BY month ...)` و درآمد ماه قبل همان شهر با `lagInFrame` روی پارتیشن `city` محاسبه شد. برای ماه اول سال (`202501`) که ماه قبلی وجود ندارد و مقدار `prev_month_rev` صفر است، با شرط `if` مقدار `NULL` قرار داده شد تا خطای تقسیم بر صفر یا درصد غیرواقعی تولید نشود.

---

### ۷. تعداد مشتری‌های جدید هر ماه
#### کوئری:
```sql
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
```
#### توضیح:
در ساب‌کوئری داخلی اولین ماه خرید هر مشتری با `min(toYYYYMM(created_at))` پیدا شد و در کوئری بیرونی تعداد مشتریان به تفکیک ماهِ اولین خریدشان شمارش گردید.

---

## بخش ۶: پیدا کردن کوئری کند و بهینه‌سازی

### کوئری‌ها و مراحل بهینه‌سازی:
```sql
-- ۱. بررسی ایندکس روی جدول اصلی
EXPLAIN indexes = 1
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders
WHERE customer_id = 4321
ORDER BY created_at DESC;

-- ۲. ساخت جدول کمکی و Materialized View با کلید مرتب‌سازی (customer_id, created_at)
CREATE TABLE shop.orders_by_customer AS shop.orders
ENGINE = MergeTree
ORDER BY (customer_id, created_at);

CREATE MATERIALIZED VIEW shop.orders_by_customer_mv TO shop.orders_by_customer AS
SELECT * FROM shop.orders;

INSERT INTO shop.orders_by_customer SELECT * FROM shop.orders;

-- ۳. بررسی ایندکس روی جدول بهینه شده
EXPLAIN indexes = 1
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders_by_customer
WHERE customer_id = 4321
ORDER BY created_at DESC;
```

### جدول مقایسه عملکرد (بند ۱ تا ۳):

| وضعیت | زمان اجرا | read_rows | read_bytes | Granuleهای خوانده شده / کل |
| :--- | :---: | :---: | :---: | :---: |
| **قبل از بهینه‌سازی** (`shop.orders`) | ~42 ms | 10,002,000 | ~152.60 MiB | 1226 / 1226 |
| **بعد از بهینه‌سازی** (`shop.orders_by_customer`) | ~3 ms | 8,192 | ~130.00 KiB | 1 / 1221 |

#### توضیح (بند ۲ و ۴):
* **چرا در حالت اول تمام گرانول‌ها خوانده شدند؟** چون ستون `customer_id` در `ORDER BY` جدول اصلی `orders` وجود ندارد؛ در نتیجه Sparse Index نمی‌تواند هیچ گرانولی را فیلتر کند و کل ۱۲۲۶ گرانول جدول اسکن می‌شوند. با ساخت جدول دوم (`orders_by_customer`) که با Materialized View پر می‌شود و کلیدش `(customer_id, created_at)` است، کوئری مستقیماً به ۱ گرانول هدف پرش می‌کند.
* **سنگین‌ترین کوئری‌ها در `system.query_log` و راهکار بهبود (بند ۴):** سنگین‌ترین کوئری‌های ثبت‌شده مربوط به اسکن جدول `orders_naive` و کوئری شماره ۷ بخش ۵ (پیدا کردن مشتریان جدید ماهانه که کل ۱۰ میلیون ردیف را روی `customer_id` گروه‌بندی می‌کند) هستند. برای بهینه‌سازی کوئری مشتریان جدید، می‌توان یک جدول `AggregatingMergeTree` با کلید `customer_id` و ستون `SimpleAggregateFunction(min, DateTime)` به همراه Materialized View ساخت تا تاریخ اولین خرید هر مشتری همیشه به صورت Incremental آماده باشد و نیازی به اسکن کل جدول سفارش‌ها نباشد.

---

## بخش اضافه: خواندن داده از Kafka (امتیازی)

### سؤال ۱۲: بررسی دریافت سفارش‌های جدید و Lag
#### کوئری و دستورات:
```sql
SELECT count() FROM shop.orders WHERE order_id >= 20000000;
SELECT sum(orders), sum(items), sum(revenue) FROM shop.daily_sales WHERE day = today();
SELECT database, table, assignments.partition_id, assignments.current_offset FROM system.kafka_consumers;
```
```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group clickhouse_orders
```

#### خروجی:
```text
count(): 2000
daily_sales today: 2000 orders | 6111 items | 6260428000 revenue
system.kafka_consumers: shop | orders_queue | [0,1,2] | [744,925,333]

GROUP              TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
clickhouse_orders  orders  0          744             744             0
clickhouse_orders  orders  1          925             925             0
clickhouse_orders  orders  2          333             333             0
```

#### توضیح:
تعداد ۲۰۰۰ سفارش (طی دو بار اجرای اسکریپت Producer) از طریق `orders_kafka_mv` وارد جدول `shop.orders` شدند و بلافاصله Materialized View جدول `daily_sales` هم فروش امروز را به‌روز کرد. در خروجی `kafka-consumer-groups.sh` و `system.kafka_consumers` مشاهده می‌شود که `CURRENT-OFFSET` و `LOG-END-OFFSET` در هر ۳ پارتیشن با هم برابرند (`744, 925, 333`) و مقدار `LAG` روی همه پارتیشن‌ها برابر `0` است.

---

### سؤال ۱۳: مدیریت پیام خراب در Kafka
#### کوئری:
```sql
SELECT _partition, _offset, _raw_message, _error FROM shop.orders_queue_errors;
```

#### خروجی:
```text
1   924   this is not json   Cannot parse input: expected '{' before: 'this is not json': (at row 1)
2   186   this is not json   Cannot parse input: expected '{' before: 'this is not json': (at row 1)
```

#### توضیح:
پیام‌های خراب که فرمت JSON معتبر نداشتند، به دلیل شرط `length(_error) > 0` توسط Materialized View دوم شکار شده و به همراه متن کامل پیام و علت خطا در جدول `shop.orders_queue_errors` (به عنوان Dead Letter Queue) ذخیره شدند. اگر `kafka_handle_error_mode = 'stream'` را تنظیم نکرده بودیم، مصرف‌کننده ClickHouse هنگام رسیدن به اولین پیام خراب خطا می‌داد و متوقف می‌شد و خواندن تمام پیام‌های سالم بعدی در آن پارتیشن قفل می‌گردید.

---

### سؤال ۱۴: رفتار ClickHouse هنگام ری‌استارت شدن وسط خواندن از Kafka
#### توضیح:
موتور Kafka در ClickHouse تضمین تحویل **At-Least-Once** دارد؛ یعنی پیامی گم نمی‌شود، اما اگر ClickHouse یک بچ از پیام‌ها را در جدول `orders` بنویسد و قبل از اینکه Offset جدید را در کافکا Commit کند ری‌استارت شود، بعد از بالا آمدن مجدد آن پیام‌ها را دوباره می‌خواند و رکورد تکراری وارد جدول می‌شود. برای جلوگیری از شمارش چندباره سفارش‌های تکراری در گزارش‌ها، می‌توان جدول مقصد را با موتور **`ReplacingMergeTree`** روی کلید `order_id` تعریف کرد تا رکوردهای هم‌شناسه ادغام شوند.

---

## کارهای امتیازی دیگر

### ۱. ذخیره و خواندن فایل Parquet روی MinIO با تابع `s3()`
#### کوئری:
```sql
INSERT INTO FUNCTION s3(
    'http://minio:9000/shopland/orders_202503.parquet',
    'minioadmin',
    'minioadminpassword',
    'Parquet'
)
SELECT * FROM shop.orders WHERE toYYYYMM(created_at) = 202503
SETTINGS s3_truncate_on_insert = 1;

SELECT
    count() AS total_rows_in_s3,
    sum(quantity * unit_price) AS total_amount
FROM s3(
    'http://minio:9000/shopland/orders_202503.parquet',
    'minioadmin',
    'minioadminpassword',
    'Parquet'
);
```
#### خروجی:
```text
total_rows_in_s3: 850602
total_amount:     2591482910000
```
#### توضیح:
سفارش‌های ماه مارس ۲۰۲۵ (۸۵۰,۶۰۲ رکورد) مستقیماً با تابع `s3()` به صورت فایل Parquet داخل باکت `shopland` روی سرویس Object Storage (MinIO) نوشته شدند و بدون نیاز به کپی محلی، مستقیماً از روی S3 کوئری گرفته شدند.

---

### ۲. مدیریت موجودی انبار با `CollapsingMergeTree`
#### کوئری:
```sql
INSERT INTO shop.inventory VALUES
    (101, 100, '2025-03-01 10:00:00', 1),
    (102, 50,  '2025-03-01 10:00:00', 1);

INSERT INTO shop.inventory VALUES
    (101, 100, '2025-03-01 10:00:00', -1),
    (101, 85,  '2025-03-02 12:00:00',  1);

SELECT
    product_id,
    sum(stock * sign) AS current_stock
FROM shop.inventory
GROUP BY product_id
HAVING sum(sign) > 0
ORDER BY product_id;
```
#### خروجی:
```text
101   85
102   50
```
#### توضیح:
برای تغییر موجودی کالای `101` از ۱۰۰ به ۸۵ بدون اجرای `UPDATE`، ابتدا رکورد قبلی با `sign = -1` خنثی شد و رکورد جدید با `sign = 1` درج گردید. در زمان گزارش‌گیری هم با فرمول `sum(stock * sign)` و شرط `HAVING sum(sign) > 0`، حتی قبل از اجرای Merge پس‌زمینه، موجودی دقیق لحظه‌ای محاسبه می‌شود.

---

### ۳. تجمیع سفارش‌های ۲۰۲۴ و ۲۰۲۵ با موتور `Merge`
#### کوئری:
```sql
CREATE TABLE IF NOT EXISTS shop.orders_all AS shop.orders
ENGINE = Merge('shop', '^orders(_2024)?$');

SELECT
    _table AS source_table,
    toYear(created_at) AS order_year,
    count() AS total_orders,
    sumIf(quantity * unit_price, status = 'delivered') AS total_revenue
FROM shop.orders_all
GROUP BY _table, order_year
ORDER BY order_year;
```
#### خروجی:
```text
orders_2024   2024   1000000    2019845120000
orders        2025   10000000   20199820643000
orders        2026   2000       6260428000
```
#### توضیح:
جدول `orders_all` با موتور `Merge` و عبارت باقاعده `^orders(_2024)?$` ساخته شد که خودش داده‌ای ذخیره نمی‌کند، بلکه کوئری‌ها را به صورت موازی روی هر دو جدول `orders` و `orders_2024` اجرا می‌کند. با استفاده از ستون مجازی `_table` هم مشخص شد که هر بخش از خروجی دقیقاً از کدام جدول فیزیکی خوانده شده است.