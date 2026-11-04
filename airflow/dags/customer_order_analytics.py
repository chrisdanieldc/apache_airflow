from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
import pandas as pd


@dag(
    dag_id="customer_order_analytics",
    description="Analytics: Combine customers (PG) and orders (MySQL) with transformations",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "analytics_team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["analytics", "multi-db", "clickhouse"],
)
def customer_order_analytics():

    @task
    def extract_customers():
        """Extract customers from PostgreSQL"""
        print("Extracting customers from PostgreSQL...")

        hook = PostgresHook(postgres_conn_id="ecommerce_postgres")

        sql = """
            SELECT
                customer_id,
                email,
                first_name,
                last_name,
                city,
                country,
                created_at
            FROM customers
        """

        df = hook.get_pandas_df(sql)
        print(f"Extracted {len(df)} customers")
        return df

    @task
    def extract_orders():
        """Extract orders from MySQL"""
        print("Extracting orders from MySQL...")

        hook = MySqlHook(mysql_conn_id="ecommerce_mysql")

        sql = """
            SELECT
                order_id,
                customer_id,
                order_status,
                total_amount,
                order_date,
                created_at
            FROM orders
        """

        df = hook.get_pandas_df(sql)
        print(f"Extracted {len(df)} orders")
        return df

    @task
    def transform_data(customers_df, orders_df):
        """Transform: Calculate customer analytics"""
        print("Transforming data...")

        # Join customers with orders
        merged = orders_df.merge(
            customers_df,
            on='customer_id',
            how='left'
        )

        # Calculate analytics per customer
        analytics = merged.groupby('customer_id').agg({
            'order_id': 'count',
            'total_amount': 'sum',
            'order_date': 'max'
        }).reset_index()

        # Rename columns
        analytics.columns = [
            'customer_id',
            'total_orders',
            'total_spent',
            'last_order_date'
        ]

        # Merge back with customer details
        result = customers_df.merge(
            analytics,
            on='customer_id',
            how='left'
        )

        # Fill NaN for customers with no orders
        result['total_orders'] = result['total_orders'].fillna(0).astype(int)
        result['total_spent'] = result['total_spent'].fillna(0)

        # Filter to only customers with at least one order (for analytics purposes)
        result = result[result['total_orders'] > 0].copy()

        # Calculate average order value
        result['avg_order_value'] = result['total_spent'] / result['total_orders']

        # Categorize customers
        result['customer_segment'] = result['total_spent'].apply(
            lambda x: 'Premium' if x > 1000 else 'Standard' if x > 500 else 'Basic'
        )

        print(f"Transformed {len(result)} customer records")
        print(f"\nCustomer segments:")
        print(result['customer_segment'].value_counts())

        return result

    @task
    def load_to_clickhouse(data):
        """Load analytics to ClickHouse"""
        print("Loading to ClickHouse...")

        # Get connection
        conn = BaseHook.get_connection("ecommerce_clickhouse")

        client = Client(
            host=conn.host,
            port=9000,
            user=conn.login,
            password=conn.password,
            database='default'
        )

        # Create table
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

        # Truncate old data
        client.execute("TRUNCATE TABLE customer_analytics")

        # Prepare records
        records = data[[
            'customer_id', 'email', 'first_name', 'last_name',
            'city', 'country', 'total_orders', 'total_spent',
            'avg_order_value', 'last_order_date', 'customer_segment',
            'created_at'
        ]].to_dict('records')

        # Insert data
        client.execute(
            """INSERT INTO customer_analytics
               (customer_id, email, first_name, last_name, city, country,
                total_orders, total_spent, avg_order_value, last_order_date,
                customer_segment, created_at) VALUES""",
            records
        )

        # Verify
        count = client.execute("SELECT COUNT(*) FROM customer_analytics")[0][0]
        print(f"Loaded {count} records to ClickHouse")

        # Show sample statistics
        stats = client.execute("""
            SELECT
                customer_segment,
                COUNT(*) as customers,
                SUM(total_orders) as orders,
                SUM(total_spent) as revenue
            FROM customer_analytics
            GROUP BY customer_segment
            ORDER BY revenue DESC
        """)

        print("\nSegment Statistics:")
        for row in stats:
            print(f"  {row[0]}: {row[1]} customers, {row[2]} orders, ${row[3]:.2f} revenue")

        return count

    # Define task flow
    customers = extract_customers()
    orders = extract_orders()
    transformed = transform_data(customers, orders)
    load_to_clickhouse(transformed)


customer_order_analytics()
