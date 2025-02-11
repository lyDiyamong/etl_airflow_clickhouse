FROM apache/airflow:latest

USER airflow

# Install PostgreSQL and ClickHouse drivers
RUN pip install psycopg2-binary clickhouse-driver airflow-provider-clickhouse pymongo python-dotenv
