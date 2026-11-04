#!/usr/bin/env python3
"""
E-commerce Data Generator
Generates realistic test data for the e-commerce database using Faker.
"""

import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
import mysql.connector
from decimal import Decimal
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Faker
fake = Faker()

# Database connection parameters
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': os.getenv('POSTGRES_DB', 'ecommerce_pg'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres123')
}

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'database': os.getenv('MYSQL_DATABASE', 'ecommerce_mysql'),
    'user': os.getenv('MYSQL_USER', 'mysql_user'),
    'password': os.getenv('MYSQL_PASSWORD', 'mysql123')
}

# Generation parameters
NUM_CUSTOMERS = 10000
NUM_PRODUCTS = 1000
NUM_ORDERS = 50000


def connect_postgres():
    """Connect to PostgreSQL database."""
    print("Connecting to PostgreSQL...")
    return psycopg2.connect(**POSTGRES_CONFIG)


def connect_mysql():
    """Connect to MySQL database."""
    print("Connecting to MySQL...")
    return mysql.connector.connect(**MYSQL_CONFIG)


def generate_customers(pg_conn):
    """Generate customer data in PostgreSQL."""
    print(f"Generating {NUM_CUSTOMERS} customers...")
    cursor = pg_conn.cursor()

    customers = []
    for i in range(NUM_CUSTOMERS):
        customer = (
            fake.unique.email(),
            fake.first_name(),
            fake.last_name(),
            fake.phone_number()[:20],
            fake.street_address(),
            fake.secondary_address() if random.random() > 0.7 else None,
            fake.city(),
            fake.state(),
            fake.postcode(),
            fake.country(),
        )
        customers.append(customer)

        if (i + 1) % 1000 == 0:
            print(f"  Generated {i + 1} customers...")

    cursor.executemany(
        """
        INSERT INTO customers
        (email, first_name, last_name, phone, address_line1, address_line2,
         city, state, postal_code, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        customers
    )

    pg_conn.commit()
    cursor.close()
    print(f"✓ Created {NUM_CUSTOMERS} customers")


def generate_products(mysql_conn):
    """Generate product data in MySQL."""
    print(f"Generating {NUM_PRODUCTS} products...")
    cursor = mysql_conn.cursor()

    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden',
                  'Sports', 'Toys', 'Beauty', 'Food & Beverage']

    products = []
    for i in range(NUM_PRODUCTS):
        product = (
            fake.catch_phrase(),
            random.choice(categories),
            fake.text(max_nb_chars=200),
            round(random.uniform(9.99, 999.99), 2),
            random.randint(0, 1000),
            fake.unique.ean13(),
        )
        products.append(product)

        if (i + 1) % 200 == 0:
            print(f"  Generated {i + 1} products...")

    cursor.executemany(
        """
        INSERT INTO products
        (product_name, category, description, price, stock_quantity, sku)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        products
    )

    mysql_conn.commit()
    cursor.close()
    print(f"✓ Created {NUM_PRODUCTS} products")


def generate_orders_and_related_data(mysql_conn, pg_conn):
    """Generate orders, order items, payments, refunds, and shipments."""
    print(f"Generating {NUM_ORDERS} orders and related data...")

    mysql_cursor = mysql_conn.cursor()
    pg_cursor = pg_conn.cursor()

    # Order statuses with weights
    order_statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
    status_weights = [0.05, 0.10, 0.15, 0.65, 0.05]

    payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
    payment_statuses = ['completed', 'pending', 'failed']

    shipment_carriers = ['FedEx', 'UPS', 'USPS', 'DHL']
    shipment_statuses = ['pending', 'in_transit', 'delivered', 'returned']

    start_date = datetime.now() - timedelta(days=365)

    for i in range(NUM_ORDERS):
        # Generate order
        customer_id = random.randint(1, NUM_CUSTOMERS)
        order_status = random.choices(order_statuses, weights=status_weights)[0]
        order_date = fake.date_time_between(start_date=start_date, end_date='now')

        # Generate 1-5 order items
        num_items = random.randint(1, 5)
        order_items = []
        total_amount = Decimal('0.00')

        for _ in range(num_items):
            product_id = random.randint(1, NUM_PRODUCTS)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(9.99, 299.99), 2)
            subtotal = round(quantity * unit_price, 2)
            total_amount += Decimal(str(subtotal))

            order_items.append((product_id, quantity, unit_price, subtotal))

        # Insert order
        mysql_cursor.execute(
            """
            INSERT INTO orders (customer_id, order_status, total_amount, order_date)
            VALUES (%s, %s, %s, %s)
            """,
            (customer_id, order_status, float(total_amount), order_date)
        )
        order_id = mysql_cursor.lastrowid

        # Insert order items
        for product_id, quantity, unit_price, subtotal in order_items:
            mysql_cursor.execute(
                """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (order_id, product_id, quantity, unit_price, subtotal)
            )

        # Generate payment (most orders have payments)
        if random.random() > 0.02:  # 98% of orders have payments
            payment_status = random.choices(
                payment_statuses,
                weights=[0.85, 0.10, 0.05]
            )[0]
            payment_method = random.choice(payment_methods)
            payment_date = order_date + timedelta(hours=random.randint(0, 24))
            transaction_id = fake.uuid4() if payment_status == 'completed' else None

            pg_cursor.execute(
                """
                INSERT INTO payments
                (order_id, payment_method, payment_status, amount, transaction_id, payment_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING payment_id
                """,
                (order_id, payment_method, payment_status, float(total_amount),
                 transaction_id, payment_date)
            )
            payment_id = pg_cursor.fetchone()[0]

            # Generate refunds (5% of completed payments get refunded)
            if payment_status == 'completed' and random.random() < 0.05:
                refund_type = random.choice(['full', 'partial'])
                refund_amount = float(total_amount) if refund_type == 'full' else round(float(total_amount) * random.uniform(0.2, 0.8), 2)
                refund_status = random.choice(['completed', 'pending'])
                refund_date = payment_date + timedelta(days=random.randint(1, 30))
                refund_reasons = [
                    'Customer request', 'Damaged product', 'Wrong item shipped',
                    'Product defective', 'Changed mind'
                ]

                pg_cursor.execute(
                    """
                    INSERT INTO refunds
                    (payment_id, refund_amount, refund_reason, refund_status, refund_type, refund_date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (payment_id, refund_amount, random.choice(refund_reasons),
                     refund_status, refund_type, refund_date)
                )

        # Generate shipment (for non-cancelled orders)
        if order_status != 'cancelled' and random.random() > 0.05:
            if order_status == 'delivered':
                shipment_status = 'delivered'
            elif order_status == 'shipped':
                shipment_status = random.choice(['in_transit', 'delivered'])
            else:
                shipment_status = random.choice(['pending', 'in_transit'])

            shipped_date = order_date + timedelta(days=random.randint(1, 3)) if shipment_status != 'pending' else None
            estimated_delivery = order_date + timedelta(days=random.randint(5, 10))
            actual_delivery = shipped_date + timedelta(days=random.randint(3, 7)) if shipment_status == 'delivered' else None

            mysql_cursor.execute(
                """
                INSERT INTO shipments
                (order_id, tracking_number, carrier, shipment_status,
                 shipped_date, estimated_delivery_date, actual_delivery_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (order_id, fake.uuid4()[:20], random.choice(shipment_carriers),
                 shipment_status, shipped_date, estimated_delivery, actual_delivery)
            )

        if (i + 1) % 5000 == 0:
            mysql_conn.commit()
            pg_conn.commit()
            print(f"  Generated {i + 1} orders...")

    mysql_conn.commit()
    pg_conn.commit()

    mysql_cursor.close()
    pg_cursor.close()

    print(f"✓ Created {NUM_ORDERS} orders with order items, payments, refunds, and shipments")


def print_statistics(mysql_conn, pg_conn):
    """Print statistics about generated data."""
    print("\n" + "="*60)
    print("DATA GENERATION STATISTICS")
    print("="*60)

    mysql_cursor = mysql_conn.cursor()
    pg_cursor = pg_conn.cursor()

    # PostgreSQL stats
    pg_cursor.execute("SELECT COUNT(*) FROM customers")
    print(f"\nPostgreSQL (ecommerce_pg):")
    print(f"  Customers:      {pg_cursor.fetchone()[0]:,}")

    pg_cursor.execute("SELECT COUNT(*) FROM payments")
    print(f"  Payments:       {pg_cursor.fetchone()[0]:,}")

    pg_cursor.execute("SELECT COUNT(*) FROM refunds")
    print(f"  Refunds:        {pg_cursor.fetchone()[0]:,}")

    # MySQL stats
    mysql_cursor.execute("SELECT COUNT(*) FROM products")
    print(f"\nMySQL (ecommerce_mysql):")
    print(f"  Products:       {mysql_cursor.fetchone()[0]:,}")

    mysql_cursor.execute("SELECT COUNT(*) FROM orders")
    print(f"  Orders:         {mysql_cursor.fetchone()[0]:,}")

    mysql_cursor.execute("SELECT COUNT(*) FROM order_items")
    print(f"  Order Items:    {mysql_cursor.fetchone()[0]:,}")

    mysql_cursor.execute("SELECT COUNT(*) FROM shipments")
    print(f"  Shipments:      {mysql_cursor.fetchone()[0]:,}")

    print("\n" + "="*60)

    mysql_cursor.close()
    pg_cursor.close()


def main():
    """Main function to generate all data."""
    print("\n" + "="*60)
    print("E-COMMERCE DATA GENERATOR")
    print("="*60 + "\n")

    try:
        # Connect to databases
        pg_conn = connect_postgres()
        mysql_conn = connect_mysql()

        # Generate data
        generate_customers(pg_conn)
        generate_products(mysql_conn)
        generate_orders_and_related_data(mysql_conn, pg_conn)

        # Print statistics
        print_statistics(mysql_conn, pg_conn)

        # Close connections
        pg_conn.close()
        mysql_conn.close()

        print("\n✓ Data generation completed successfully!\n")

    except Exception as e:
        print(f"\n✗ Error: {e}\n")
        raise


if __name__ == "__main__":
    main()
