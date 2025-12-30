import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "umar"
DAG_ID = "ecommerce_ods_pg"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "ecommerce"
SCHEMA = "ods"
TARGET_TABLE = "fct_products"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# DuckDB
PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
# Загрузка e-commerce данных из S3 в Postgres ODS
Задача: Загрузить данные товаров из MinIO (S3) в Postgres
"""

SHORT_DESCRIPTION = "Load e-commerce data from MinIO to Postgres ODS"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 12, 25, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_raw_data_to_ods_pg(**context):
    """Загрузка товаров из S3 в Postgres"""

    # 1. Получаем дату КАК СТРОКУ
    start_date, end_date = get_dates(**context)

    logging.info(f"🚀 Start e-commerce data load for: {start_date}/{end_date}")

    con = duckdb.connect()

    # 2. Настройка S3 (MinIO)
    con.sql(f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE 'postgres',
            USER 'postgres',
            PASSWORD '{PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

        CREATE SCHEMA IF NOT EXISTS dwh_postgres_db.{SCHEMA};

        CREATE TABLE IF NOT EXISTS dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} (
        product_id integer PRIMARY KEY,
        product_name TEXT,
        description TEXT,
        price DOUBLE,
        discount_pct DOUBLE,
        rating DOUBLE,
        stock Integer,
        brand TEXT,
        category TEXT
            );

        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
        product_id,
        product_name,
        description,
        price,
        discount_pct,
        rating,
        stock,
        brand,
        category
        )
        SELECT
        p.id,
        p.title,
        p.description,
        p.price,
        p.discountPercentage,
        p.rating,
        p.stock,
        p.brand,
        p.category
        FROM (
            SELECT unnest(products) as p
        FROM read_parquet('s3://supfun/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet')
        )
        ON CONFLICT (product_id) do UPDATE SET
        product_name = EXCLUDED.product_name,
        description = EXCLUDED.description,
        price = EXCLUDED.price,
        discount_pct = EXCLUDED.discount_pct,
        rating = EXCLUDED.rating,
        stock = EXCLUDED.stock,
        brand = EXCLUDED.brand,
        category = EXCLUDED.category;
        """,
        )

    con.close()
    logging.info(f"✅ E-commerce data loaded to Postgres: {start_date}")



with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",  # На 1 час позже raw_s3_api (05:00 → 06:00)
    default_args=args,
    tags=["s3", "ods", "pg", "ecommerce"],
    description=SHORT_DESCRIPTION,

    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    # Sensor ждет raw_s3_api за ВЧЕРАШНИЙ день
    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_s3_api",  # ID первого DAG
        allowed_states=["success"],
        mode="reschedule",
        timeout=36000,  # 1 час максимум
        poke_interval=60,  # Проверять каждые 60 cекунд
    )

    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(task_id="end")

    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end