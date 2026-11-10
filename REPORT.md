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