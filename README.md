# ClickHouse E-Commerce Data Warehouse & Real-Time Pipeline

An end-to-end analytical data warehouse (OLAP) and real-time streaming ingestion pipeline designed to process 10M+ transaction records for an e-commerce platform (**ShopLand**). 

The system leverages **ClickHouse**, **Apache Kafka**, **PostgreSQL**, and **MinIO** to deliver sub-second analytical reporting, fault-tolerant event streaming, automated pre-aggregations, and granular I/O optimization.

---

## 🏛 Architecture Overview

```
                      +-----------------------------+
                      |   PostgreSQL (Metadata)     |
                      |   5,000 Catalog Products    |
                      +--------------+--------------+
                                     |
                                     | (In-Memory Dictionary Cache via dictGet)
                                     v
+-------------------+      +-------------------+      +------------------------+
|   Apache Kafka    | ---> | ClickHouse Kafka  | ---> |   shop.orders          |
|  (orders topic)   |      | Engine & Parser   |      |  (10M+ Core MergeTree) |
+-------------------+      +---------+---------+      +-----------+------------+
                                     |                            |
                     (DLQ on Corrupt)|               +------------+------------+
                                     v               |                         |
                           +-------------------+     v                         v
                           | orders_errors     |  SummingMergeTree     AggregatingMergeTree
                           | (Dead Letter Queue) (daily_sales)        (daily_customers)
                                                     |                         |
                                                     v                         v
                                             Daily Revenue Metrics      Monthly Unique Active
                                             & Top Product Reports      Customers (HyperLogLog)
```

---

## 🚀 Key Features & Engineering Highlights

* **High-Throughput Storage Engine Design:**
  * Modeled partitioned, columnar tables using `MergeTree` engines.
  * Applied specialized compression codecs (`Delta`, `DoubleDelta`, and `ZSTD`) alongside `LowCardinality` and `Enum8` types, cutting physical disk usage by over 70% compared to baseline storage.
* **Fault-Tolerant Streaming Ingestion:**
  * Integrated ClickHouse directly with a multi-partitioned Apache Kafka topic.
  * Implemented an automated **Dead Letter Queue (DLQ)** pattern using `kafka_handle_error_mode = 'stream'` to isolate malformed payloads without halting ingestion pipelines.
* **Incremental & Materialized Pre-Aggregations:**
  * Designed automated incremental rollups using `SummingMergeTree` for multi-dimensional revenue metrics.
  * Utilized `AggregatingMergeTree` with state combinators (`uniqState` / `uniqMerge` via HyperLogLog) to compute multi-month distinct customer metrics in milliseconds.
* **Zero-Network Join Acceleration:**
  * Synchronized PostgreSQL catalog tables via ClickHouse in-memory **Dictionaries (`dictGet`)** with dynamic `LIFETIME` refresh intervals, replacing heavy distributed network joins with $O(1)$ memory lookups.
* **Query Performance Profiling & Optimization:**
  * Isolated slow queries using `system.query_log` and `EXPLAIN indexes = 1`.
  * Reduced customer-lookup scan times from **~45ms (10M rows scanned across 1,221 granules)** to **~3ms (1 granule scan)** by engineering a secondary indexing projection driven by Materialized Views.
* **Storage Tiering (Data Lakehouse Integration):**
  * Built cold-data archiving pipelines pushing partitioned Parquet datasets directly to MinIO Object Storage (S3-compatible API).

---

## 📊 Performance & Optimization Benchmarks

### 1. Storage Optimization (Column Compression & Data Types)
By moving from unindexed string formats (`orders_naive`) to strict integer/enum representations and advanced encoding schemes, overall storage footprint was reduced dramatically:

| Metric | Baseline (`shop.orders_naive`) | Optimized (`shop.orders`) | Improvement |
| :--- | :---: | :---: | :---: |
| **Row Count** | 10,000,000 | 10,000,000 | — |
| **Total Disk Size** | ~480 MiB | ~135 MiB | **~72% reduction** |
| **Primary Codecs** | Default (LZ4) | `Delta`, `DoubleDelta`, `ZSTD(1)` | Custom encodings |

### 2. Primary Index & Granule Pruning (Customer History Lookup)
* **Query:** Filter transaction history for a specific customer (`WHERE customer_id = 4321 ORDER BY created_at DESC`).
* **Root Cause:** Primary table sorting key did not prefix `customer_id`, forcing full scans across all granules.
* **Solution:** Provisioned `shop.orders_by_customer` sorted by `(customer_id, created_at)` continuously synced via Materialized Views.

| Stage | Execution Time | Rows Read | Bytes Read | Granules Read / Total |
| :--- | :---: | :---: | :---: | :---: |
| **Before Optimization** | `~45 ms` | 10,000,000 | `~55.00 MiB` | 1221 / 1221 (Full Scan) |
| **After Optimization** | `~3 ms` | 8,192 | `~180.00 KiB` | **1 / 1221 (Index Pruned)** |

---

## 🛠 Tech Stack

* **OLAP Database:** ClickHouse 24.8+ / 26.8
* **Message Broker:** Apache Kafka 4.0 (KRaft mode)
* **RDBMS Source:** PostgreSQL 17
* **Object Storage:** MinIO (S3 API compatible)
* **Containerization & Orchestration:** Docker Compose
* **Query Language:** ClickHouse SQL Dialect, Analytical Window Functions

---

## 📁 Repository Structure

```text
.
├── docker-compose.yml              # Multi-container orchestration (ClickHouse, Kafka, PG, MinIO)
├── postgres/
│   └── seed.sql                    # Initial product catalog schema & data generation
├── sql/
│   ├── 01_schema.sql               # Core MergeTree table definitions, types & compression codecs
│   ├── 02_load.sql                 # Data generation (10M rows), Parquet export, batch inserts
│   ├── 03_changes.sql              # ReplacingMergeTree, Lightweight Deletes & TTL policies
│   ├── 04_views_and_dictionary.sql # Summing/Aggregating MVs, PG engine & ClickHouse Dictionary
│   ├── 05_queries.sql              # Analytical reporting queries (Window functions, LIMIT BY)
│   ├── 06_performance.sql          # Query profiling (query_log, EXPLAIN) & secondary tables
│   └── 07_kafka.sql                # Kafka engine, dead letter queue, MinIO S3 & CollapsingMergeTree
└── README.md
```

---

## 🚦 Getting Started

### Prerequisites
* Docker Engine 24.0+
* Docker Compose v2.20+
* `curl` and `bash`

### 1. Provision Infrastructure
Clone the repository and start the cluster:
```bash
git clone https://github.com/<your-username>/clickhouse-ecommerce-dwh.git
cd clickhouse-ecommerce-dwh

docker compose up -d
```

Verify all services (`postgres`, `clickhouse`, `kafka`, `minio`) are healthy:
```bash
docker compose ps
```

### 2. Execute Migrations & Pipelines
Run the SQL scripts in sequential order:
```bash
# 1. Initialize schema & tables
docker compose exec -T clickhouse clickhouse-client -u student --password student --multiquery < sql/01_schema.sql

# 2. Ingest 10M rows & verify compression
docker compose exec -T clickhouse clickhouse-client -u student --password student --multiquery < sql/02_load.sql

# 3. Apply mutations, deduplication & TTL demo
docker compose exec -T clickhouse clickhouse-client -u student --password student --multiquery < sql/03_changes.sql

# 4. Create Materialized Views & Dictionary connections
docker compose exec -T clickhouse clickhouse-client -u student --password student --multiquery < sql/04_views_and_dictionary.sql

# 5. Run analytical business reports
docker compose exec -T clickhouse clickhouse-client -u student --password student --multiquery < sql/05_queries.sql

# 6. Profile query performance & build indexing projection
docker compose exec -T clickhouse clickhouse-client -u student --password student --multiquery < sql/06_performance.sql
```

### 3. Test Real-Time Kafka Streaming & DLQ
Create the Kafka topic and configure streaming pipelines:
```bash
# Create topic
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --topic orders --partitions 3

# Apply Kafka engine and Materialized View
docker compose exec -T clickhouse clickhouse-client -u student --password student --multiquery < sql/07_kafka.sql
```

Produce test traffic (1,000 valid events + 1 malformed payload):
```bash
# Send valid JSON orders
docker compose exec -T clickhouse clickhouse-client -u student --password student -q "
SELECT 20000000 + number AS order_id, rand(1) % 200000 + 1 AS customer_id,
rand(2) % 5000 + 1 AS product_id, 'Tehran' AS city, 'delivered' AS status,
rand(5) % 5 + 1 AS quantity, (rand(6) % 2000 + 10) * 1000 AS unit_price,
now() AS created_at
FROM numbers(1000) FORMAT JSONEachRow" | \
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
--bootstrap-server kafka:9092 --topic orders

# Send malformed string
echo 'invalid_json_payload' | docker compose exec -T kafka \
/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic orders
```

Verify real-time ingestion and DLQ routing:
```sql
-- Valid orders reached primary table:
SELECT count() FROM shop.orders WHERE order_id >= 20000000;

-- Dead letter queue isolated corrupt records:
SELECT _raw_message, _error FROM shop.orders_queue_errors;
```