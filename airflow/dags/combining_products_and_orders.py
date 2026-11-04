from datetime import datetime, timedelta
import pandas as pd

from airflow.decorators import dag, task

MYSQL_CONN_ID = "ecommerce_mysql"

@dag(
    dag_id="combining_products_and_orders",
    description="Combine products and orders data",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # trigger manually for now
    catchup=False,
    default_args={
        "owner": "Combining Products and Orders",
        "retries": 2,
        "retry_delay": timedelta(seconds=30), 
        },
        tags=["product", "orders"], 
)
def combining_products_and_orders():
    
    @task
    def fetch_products():
        from airflow.providers.mysql.hooks.mysql import MySqlHook   
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        products = hook.get_pandas_df("SELECT * FROM products")
        print(f"Fetched {len(products)} products")
        return products

    @task
    def fetch_order_items():
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        order_items = hook.get_pandas_df("SELECT * FROM order_items")
        print(f"Fetched {len(order_items)} order items")
        return order_items

    @task
    def join_order_items_and_products(order_items_df, products_df):
        """Join order_items with products on product_id"""
        print("Joining order_items and products...")

        # Inner join on product_id (equivalent to SQL INNER JOIN)
        result = order_items_df.merge(
            products_df,
            on='product_id',
            how='inner'
        )

        print(f"Created {len(result)} joined records")
        print(f"\nFirst 5 records:")
        print(result.head())
        return result

    @task
    def load_to_clickhouse(data):
        """Load joined data to ClickHouse"""
        print("Loading to ClickHouse...")

        from airflow.hooks.base import BaseHook
        from clickhouse_driver import Client

        # Get ClickHouse connection details
        conn = BaseHook.get_connection("ecommerce_clickhouse")

        client = Client(
            host=conn.host,
            port=9000,
            user=conn.login,
            password=conn.password,
            database='default'
        )

        # Create table if not exists
        client.execute("""
            CREATE TABLE IF NOT EXISTS order_items_products (
                order_item_id UInt32,
                order_id UInt32,
                product_id UInt32,
                quantity UInt16,
                unit_price Decimal(10, 2),
                product_name String,
                category String,
                price Decimal(10, 2),
                stock_quantity UInt32,
                loaded_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY order_id
        """)

        # Clear old data
        client.execute("TRUNCATE TABLE order_items_products")

        # Insert new data
        records = data.to_dict('records')
        client.execute(
            """INSERT INTO order_items_products
               (order_item_id, order_id, product_id, quantity, unit_price,
                product_name, category, price, stock_quantity) VALUES""",
            records
        )

        count = client.execute("SELECT COUNT(*) FROM order_items_products")[0][0]
        print(f"Loaded {count} records to ClickHouse")
        return count

    # Define task flow
    products = fetch_products()
    order_items = fetch_order_items()
    joined_data = join_order_items_and_products(order_items, products)
    load_to_clickhouse(joined_data)


combining_products_and_orders()