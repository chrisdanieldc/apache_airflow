# Multi-Database E-Commerce Data Platform

A production-grade data engineering platform demonstrating polyglot persistence, ETL orchestration, and real-time analytics using modern open-source technologies.

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Technology Stack](#technology-stack)
- [Data Model](#data-model)
- [Installation](#installation)
- [ETL Pipeline](#etl-pipeline)
- [Database Access](#database-access)
- [Query Examples](#query-examples)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Overview

This platform implements a microservices-inspired data architecture where domain-specific data resides in purpose-built databases. The system demonstrates:

- **Polyglot Persistence**: PostgreSQL for transactional data, MySQL for operational data, ClickHouse for analytics
- **ETL Orchestration**: Apache Airflow with TaskFlow API for data pipeline management
- **Cross-Database Integration**: Shared business identifiers enable distributed joins
- **Analytics Layer**: ClickHouse-based OLAP storage for aggregated reporting

### Key Metrics

- **Data Volume**: 10K customers, 1K products, 50K orders, 150K order items
- **Refresh Frequency**: Daily batch ETL (configurable via Airflow schedules)
- **Data Latency**: Sub-minute for batch pipelines
- **Storage Architecture**: Separate OLTP (PostgreSQL/MySQL) and OLAP (ClickHouse) layers

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources (OLTP)                      │
├────────────────────────┬────────────────────────────────────────┤
│   PostgreSQL:5432      │         MySQL:3306                     │
│   ─────────────────    │         ─────────────                  │
│   • customers          │         • products                     │
│   • payments           │         • orders                       │
│   • refunds            │         • order_items                  │
│                        │         • shipments                    │
└────────────┬───────────┴─────────────┬──────────────────────────┘
             │                         │
             │    ┌────────────────────┘
             │    │
             ▼    ▼
      ┌──────────────────┐
      │ Apache Airflow   │
      │ ────────────────  │
      │ • Webserver:8080 │
      │ • Scheduler      │
      │ • LocalExecutor  │
      │ • PostgreSQL DB  │
      └─────────┬────────┘
                │
                ▼
      ┌─────────────────────┐
      │  ClickHouse:9000    │
      │  ───────────────    │
      │  Analytics Layer    │
      │  • customer_orders  │
      │  • customer_analytics│
      │  • order_items_products │
      └─────────────────────┘
```

### Network Architecture

- **Network**: `ecommerce_network` (bridge mode)
- **Service Discovery**: Docker internal DNS
- **Port Mapping**:
  - PostgreSQL: `5432:5432`
  - MySQL: `3306:3306`
  - ClickHouse: `8123:8123` (HTTP), `9000:9000` (Native)
  - Airflow: `8080:8080` (Web UI)

### Container Health Checks

All services implement health check endpoints:
- **PostgreSQL**: `pg_isready` (10s interval)
- **MySQL**: `mysqladmin ping` (10s interval)
- **ClickHouse**: HTTP `/ping` endpoint (10s interval)
- **Airflow**: `/health` endpoint (30s interval, 60s startup)

## Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| OLTP Database | PostgreSQL | 15 (Alpine) | Customer & payment domain |
| OLTP Database | MySQL | 8.0 | Product & order domain |
| OLAP Database | ClickHouse | 23.8 | Analytics & aggregations |
| Orchestration | Apache Airflow | 2.7.3 | ETL pipeline management |
| Runtime | Python | 3.11 | Data processing & DAGs |
| Containerization | Docker Compose | 3.8 | Infrastructure orchestration |

### Python Dependencies

```
apache-airflow-providers-postgres==5.7.1
apache-airflow-providers-mysql==5.5.3
clickhouse-driver==0.2.7
psycopg2-binary
mysql-connector-python
pandas
Faker
```

## Data Model

### Domain Separation

**Customer & Payment Domain (PostgreSQL)**
```sql
customers (customer_id PK, email, name, address, timestamps)
    ↓ (1:N)
payments (payment_id PK, order_id, customer_id, method, status, amount)
    ↓ (1:N)
refunds (refund_id PK, payment_id FK, amount, type, timestamps)
```

**Product & Order Domain (MySQL)**
```sql
products (product_id PK, name, category, price, stock)
    ↓ (N:M via order_items)
orders (order_id PK, customer_id, status, total_amount, timestamps)
    ↓ (1:N)
order_items (order_item_id PK, order_id FK, product_id FK, quantity, price)
    ↓ (1:1)
shipments (shipment_id PK, order_id FK, status, carrier, tracking)
```

### Cross-Database References

Implemented via **shared business identifiers** (no foreign key constraints):

- `orders.customer_id` → `customers.customer_id` (MySQL → PostgreSQL)
- `payments.order_id` → `orders.order_id` (PostgreSQL → MySQL)

This pattern enables:
- Independent scaling of databases
- Technology-specific optimizations
- Bounded contexts (DDD pattern)
- Realistic microservices data architecture

## Installation

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- Python 3.8+ (for data generation)
- 4GB RAM minimum (8GB recommended)

### Quick Start

1. **Clone and navigate to repository**
```bash
cd /path/to/apache_airflow
```

2. **Start infrastructure**
```bash
docker-compose up -d postgres mysql clickhouse
```

**What happens during startup:**
- **PostgreSQL**: Executes `postgres/init.sql` (creates customers, payments, refunds tables)
- **MySQL**: Executes `mysql/init.sql` (creates products, orders, order_items, shipments tables)
- **ClickHouse**: Executes scripts from `clickhouse/init/` directory (if any), creates default database

3. **Verify database health**
```bash
docker-compose ps
# Wait until all show "healthy" status (10-15 seconds)

# Verify ClickHouse is ready
docker exec ecommerce_clickhouse clickhouse-client --query "SELECT version()"

# Verify PostgreSQL
docker exec ecommerce_postgres psql -U postgres -d ecommerce_pg -c "\dt"

# Verify MySQL
docker exec ecommerce_mysql mysql -u mysql_user -pmysql123 ecommerce_mysql -e "SHOW TABLES;"
```

4. **Initialize Airflow metadata database**

Airflow requires a dedicated PostgreSQL database (`airflow_db`) separate from the ecommerce data.

**Automated setup:**
```bash
chmod +x init_airflow_db.sh
./init_airflow_db.sh
```

**Manual setup:**
```bash
# Create airflow_db in PostgreSQL
docker exec ecommerce_postgres psql -U postgres -c "CREATE DATABASE airflow_db;"

# Verify creation
docker exec ecommerce_postgres psql -U postgres -c "\l"
```

The Airflow services will automatically:
- Run database migrations on first startup (`airflow db upgrade`)
- Create required tables (dag, task_instance, log, etc.)
- Initialize metadata schema

**Connection configured in .env:**
```bash
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres123@postgres:5432/airflow_db
```

5. **Start Airflow services**
```bash
docker-compose up -d airflow-webserver airflow-scheduler

# Monitor initialization (first startup takes 60-90 seconds)
docker-compose logs -f airflow-webserver airflow-scheduler
```

6. **Install Python dependencies**
```bash
pip install -r requirements.txt
```

7. **Generate sample data**
```bash
python generate_data.py
# Runtime: ~2-5 minutes
# Generates: 10K customers, 1K products, 50K orders + related data
```

8. **Create Airflow admin user**
```bash
docker exec ecommerce_airflow_webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin123
```

9. **Access Airflow UI**
```
URL: http://localhost:8080
Username: admin
Password: admin123
```

## ETL Pipeline

### Available DAGs

#### 1. `ecommerce_db_healthcheck`
**Purpose**: Database connectivity verification
**Schedule**: Manual trigger
**Tasks**:
- Checks PostgreSQL connection
- Validates table count
- Verifies server version

#### 2. `simple_customer_orders`
**Purpose**: Basic cross-database ETL
**Schedule**: @daily
**Flow**:
```
get_customers (PostgreSQL) ─┐
                            ├─> join_data ──> load_to_clickhouse
get_orders (MySQL) ─────────┘
```
**Output**: `customer_orders` table in ClickHouse

#### 3. `combining_products_and_orders`
**Purpose**: Product analytics pipeline
**Schedule**: Manual
**Flow**:
```
fetch_products (MySQL) ─────┐
                            ├─> join_order_items_and_products ──> load_to_clickhouse
fetch_order_items (MySQL) ──┘
```
**Output**: `order_items_products` table in ClickHouse

#### 4. `customer_order_analytics`
**Purpose**: Customer segmentation and RFM analysis
**Schedule**: @daily
**Flow**:
```
extract_customers (PostgreSQL) ─┐
                                ├─> transform_data ──> load_to_clickhouse
extract_orders (MySQL) ─────────┘
```
**Transformations**:
- Aggregate order count per customer
- Calculate total spent (CLV proxy)
- Compute average order value
- Customer segmentation (Basic/Standard/Premium)
- Last order date tracking

**Output**: `customer_analytics` table in ClickHouse

### ClickHouse Table Creation

Analytics tables in ClickHouse are created automatically by DAGs on first execution using `CREATE TABLE IF NOT EXISTS` pattern:

**Example from `customer_order_analytics.py`:**
```python
client.execute("""
    CREATE TABLE IF NOT EXISTS customer_analytics (
        customer_id UInt32,
        email String,
        first_name String,
        last_name String,
        city String,
        country String,
        total_orders UInt32,
        total_spent Decimal(10, 2),
        avg_order_value Decimal(10, 2),
        last_order_date DateTime,
        customer_segment String,
        created_at DateTime,
        loaded_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY customer_id
""")
```

**Table lifecycle:**
1. DAG executes → checks if table exists
2. If not exists → creates table with schema
3. Truncates existing data: `TRUNCATE TABLE <table_name>`
4. Inserts new data from ETL transformation

**Verify tables exist:**
```bash
docker exec ecommerce_clickhouse clickhouse-client --query "SHOW TABLES"
```

### Running DAGs

**Via Airflow UI:**
1. Navigate to http://localhost:8080
2. Toggle DAG to "ON"
3. Click "Trigger DAG" button

**Via CLI:**
```bash
# Test run (doesn't record state)
docker exec ecommerce_airflow_scheduler airflow dags test <dag_id>

# Production run
docker exec ecommerce_airflow_scheduler airflow dags trigger <dag_id>

# View logs
docker exec ecommerce_airflow_scheduler airflow tasks logs <dag_id> <task_id> <run_id>
```

### Airflow Connections

Pre-configured via environment variables:

```bash
# PostgreSQL
AIRFLOW_CONN_ECOMMERCE_POSTGRES=postgres://postgres:postgres123@postgres:5432/ecommerce_pg

# MySQL
AIRFLOW_CONN_ECOMMERCE_MYSQL=mysql://mysql_user:mysql123@mysql:3306/ecommerce_mysql

# ClickHouse
AIRFLOW_CONN_ECOMMERCE_CLICKHOUSE=clickhouse://default:clickhouse123@clickhouse:9000/default
```

**Usage in DAGs:**
```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="ecommerce_postgres")
df = hook.get_pandas_df("SELECT * FROM customers")
```

## Database Access

### PostgreSQL

**Connection String:**
```
postgresql://postgres:postgres123@localhost:5432/ecommerce_pg
```

**CLI Access:**
```bash
docker exec -it ecommerce_postgres psql -U postgres -d ecommerce_pg
```

**Python Access:**
```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecommerce_pg",
    user="postgres",
    password="postgres123"
)
```

### MySQL

**Connection String:**
```
mysql://mysql_user:mysql123@localhost:3306/ecommerce_mysql
```

**CLI Access:**
```bash
docker exec -it ecommerce_mysql mysql -u mysql_user -pmysql123 ecommerce_mysql
```

**Python Access:**
```python
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    database="ecommerce_mysql",
    user="mysql_user",
    password="mysql123"
)
```

### ClickHouse

**Connection String (Native):**
```
clickhouse://default:clickhouse123@localhost:9000/default
```

**Connection String (HTTP):**
```
http://default:clickhouse123@localhost:8123/
```

**CLI Access:**
```bash
docker exec -it ecommerce_clickhouse clickhouse-client
```

**Python Access:**
```python
from clickhouse_driver import Client

client = Client(
    host='localhost',
    port=9000,
    user='default',
    password='clickhouse123',
    database='default'
)

result = client.execute('SELECT * FROM customer_analytics LIMIT 10')
```

## Query Examples

### OLTP Queries (PostgreSQL)

**Customer acquisition analysis:**
```sql
SELECT
    DATE_TRUNC('month', created_at) as month,
    COUNT(*) as new_customers,
    COUNT(*) FILTER (WHERE country = 'United States') as us_customers
FROM customers
GROUP BY month
ORDER BY month DESC;
```

**Payment success rate:**
```sql
SELECT
    payment_method,
    payment_status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY payment_method), 2) as pct
FROM payments
GROUP BY payment_method, payment_status
ORDER BY payment_method, count DESC;
```

**Refund analysis:**
```sql
SELECT
    r.refund_type,
    COUNT(*) as refund_count,
    SUM(r.refund_amount) as total_refunded,
    AVG(r.refund_amount) as avg_refund,
    AVG(p.amount) as avg_original_payment
FROM refunds r
JOIN payments p ON r.payment_id = p.payment_id
GROUP BY r.refund_type;
```

### OLTP Queries (MySQL)

**Inventory status:**
```sql
SELECT
    category,
    COUNT(*) as products,
    SUM(stock_quantity) as total_stock,
    AVG(price) as avg_price,
    COUNT(*) FILTER (WHERE stock_quantity < 50) as low_stock_count
FROM products
GROUP BY category
ORDER BY total_stock DESC;
```

**Order fulfillment metrics:**
```sql
SELECT
    o.order_status,
    s.shipment_status,
    COUNT(*) as order_count,
    AVG(o.total_amount) as avg_order_value
FROM orders o
LEFT JOIN shipments s ON o.order_id = s.order_id
GROUP BY o.order_status, s.shipment_status;
```

**Top products by revenue:**
```sql
SELECT
    p.product_name,
    p.category,
    COUNT(DISTINCT oi.order_id) as times_ordered,
    SUM(oi.quantity) as units_sold,
    SUM(oi.quantity * oi.unit_price) as total_revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 20;
```

### OLAP Queries (ClickHouse)

**Customer segmentation distribution:**
```sql
SELECT
    customer_segment,
    COUNT(*) as customers,
    SUM(total_orders) as total_orders,
    SUM(total_spent) as total_revenue,
    AVG(avg_order_value) as avg_order_value,
    MAX(total_spent) as max_customer_value
FROM customer_analytics
GROUP BY customer_segment
ORDER BY total_revenue DESC;
```

**RFM cohort analysis:**
```sql
SELECT
    toYYYYMM(last_order_date) as last_order_month,
    customer_segment,
    COUNT(*) as customers,
    AVG(total_orders) as avg_orders,
    AVG(total_spent) as avg_lifetime_value
FROM customer_analytics
GROUP BY last_order_month, customer_segment
ORDER BY last_order_month DESC, customer_segment;
```

**Product performance metrics:**
```sql
SELECT
    category,
    COUNT(DISTINCT product_id) as unique_products,
    SUM(quantity) as units_sold,
    AVG(unit_price) as avg_price,
    SUM(quantity * unit_price) as revenue
FROM order_items_products
GROUP BY category
ORDER BY revenue DESC;
```

**Daily order trends:**
```sql
SELECT
    toDate(order_date) as date,
    COUNT(DISTINCT order_id) as orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_amount) as daily_revenue,
    AVG(total_amount) as avg_order_value
FROM customer_orders
GROUP BY date
ORDER BY date DESC
LIMIT 30;
```

### Cross-Database Analysis Pattern

**End-to-end customer journey:**

Step 1 - Customer profile (PostgreSQL):
```sql
SELECT customer_id, email, first_name, last_name, country, created_at
FROM customers
WHERE customer_id = 100;
```

Step 2 - Order history (MySQL):
```sql
SELECT order_id, order_status, total_amount, order_date
FROM orders
WHERE customer_id = 100
ORDER BY order_date DESC;
```

Step 3 - Payment history (PostgreSQL):
```sql
SELECT p.payment_id, p.order_id, p.payment_method, p.payment_status, p.amount
FROM payments p
WHERE p.customer_id = 100
ORDER BY p.created_at DESC;
```

Step 4 - Aggregated view (ClickHouse):
```sql
SELECT *
FROM customer_analytics
WHERE customer_id = 100;
```

## Monitoring

### Container Health

```bash
# Check all services
docker-compose ps

# View service logs
docker-compose logs -f <service_name>

# Check resource usage
docker stats
```

### Database Monitoring

**PostgreSQL:**
```sql
-- Active connections
SELECT count(*) FROM pg_stat_activity;

-- Database size
SELECT pg_size_pretty(pg_database_size('ecommerce_pg'));

-- Table sizes
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

**MySQL:**
```sql
-- Connection count
SHOW STATUS WHERE Variable_name = 'Threads_connected';

-- Database size
SELECT table_schema,
       ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'Size (MB)'
FROM information_schema.tables
WHERE table_schema = 'ecommerce_mysql'
GROUP BY table_schema;

-- Table row counts
SELECT table_name, table_rows
FROM information_schema.tables
WHERE table_schema = 'ecommerce_mysql'
ORDER BY table_rows DESC;
```

**ClickHouse:**
```sql
-- Table sizes
SELECT
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as rows
FROM system.parts
WHERE active
GROUP BY table;

-- Query performance
SELECT
    query,
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) as read_size
FROM system.query_log
WHERE type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 10;
```

### Airflow Monitoring

**Web UI Metrics:**
- DAG run status: http://localhost:8080
- Task duration trends
- Success/failure rates
- Scheduler health

**CLI Metrics:**
```bash
# DAG status
docker exec ecommerce_airflow_scheduler airflow dags list

# Task instance status
docker exec ecommerce_airflow_scheduler airflow tasks states-for-dag-run <dag_id> <run_id>

# Database check
docker exec ecommerce_airflow_scheduler airflow db check
```

## Troubleshooting

### Services Won't Start

**Issue**: Container exits immediately
```bash
# Check logs
docker-compose logs <service_name>

# Verify port availability
netstat -tuln | grep -E '3306|5432|8080|9000'

# Check disk space
df -h
```

### Database Connection Errors

**Issue**: `Connection refused` or `Host not found`
```bash
# Verify containers are healthy
docker-compose ps

# Test network connectivity
docker exec ecommerce_postgres ping -c 3 mysql
docker exec ecommerce_mysql ping -c 3 postgres

# Verify environment variables
docker exec ecommerce_airflow_scheduler env | grep AIRFLOW_CONN
```

### Airflow DAG Import Errors

**Issue**: DAGs not appearing in UI
```bash
# Check scheduler logs
docker-compose logs -f airflow-scheduler

# Verify DAG file syntax
docker exec ecommerce_airflow_scheduler python -m py_compile /opt/airflow/dags/<dag_file>.py

# Force DAG refresh
docker exec ecommerce_airflow_scheduler airflow dags reserialize
```

### Performance Issues

**Issue**: Slow query execution
```bash
# Check container resources
docker stats

# Increase memory allocation (docker-compose.yml)
deploy:
  resources:
    limits:
      memory: 2G
```

**Issue**: ETL pipeline timeout
```python
# Increase task timeout in DAG
default_args = {
    'execution_timeout': timedelta(minutes=30),
}
```

### Data Inconsistencies

**Issue**: Cross-database reference mismatches
```sql
-- Find orphaned orders (MySQL)
SELECT COUNT(*)
FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id
);

-- Find orphaned payments (PostgreSQL)
SELECT COUNT(*)
FROM payments p
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.order_id = p.order_id
);
```

### Clean Reset

**Complete environment reset:**
```bash
# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Remove images
docker-compose down --rmi all

# Restart from scratch
docker-compose up -d
python generate_data.py
```

## Project Structure

```
apache_airflow/
├── docker-compose.yml              # Service definitions
├── .env                            # Environment variables
├── requirements.txt                # Python dependencies
├── generate_data.py                # Data generator (Faker-based)
├── init_airflow_db.sh             # Airflow metadata DB initialization
│
├── postgres/
│   └── init.sql                    # PostgreSQL schema (customers, payments, refunds)
│
├── mysql/
│   └── init.sql                    # MySQL schema (products, orders, order_items, shipments)
│
├── clickhouse/
│   └── init/
│       └── create_tables.sql       # ClickHouse analytics tables
│
├── airflow/
│   ├── dags/
│   │   ├── ecommerce_db_healthcheck.py
│   │   ├── simple_customer_orders_etl.py
│   │   ├── customer_order_analytics.py
│   │   └── customer_orders_etl.py
│   ├── logs/                       # Airflow execution logs
│   ├── plugins/                    # Custom Airflow plugins
│   └── config/                     # Airflow configuration
│
├── notebooks/
│   └── airflow_connections_explained.py
│
└── README.md                       # This file
```
