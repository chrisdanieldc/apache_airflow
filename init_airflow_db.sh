#!/bin/bash
# Initialize Airflow metadata database in PostgreSQL

echo "Creating Airflow metadata database in PostgreSQL..."

docker exec ecommerce_postgres psql -U postgres -c "CREATE DATABASE airflow_db;" 2>/dev/null || echo "Database airflow_db already exists"

echo "Airflow database initialized successfully!"
