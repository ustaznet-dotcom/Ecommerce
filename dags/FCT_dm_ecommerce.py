import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "umar"
DAG_ID = "FCT_dm_ecommerce"

# Конфигурация таблиц
LAYER = "raw"  # Слой, откуда берем данные
SOURCE = "ecommerce"  # Источник данных
SOURCE_SCHEMA = "ods"  # Откуда берем данные
SOURCE_TABLE = "fct_products"
TARGET_SCHEMA = "dm"  # Куда сохраняем витрины
TARGET_TABLE = "fct_ecommerce"

# DWH подключение
PG_CONNECT = "postgres_dwh"

# Описание DAG
LONG_DESCRIPTION = """
# Витрины данных для e-commerce аналитики (DummyJSON)
"""

SHORT_DESCRIPTION = "Создание витрин данных для e-commerce аналитики"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 12, 25, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["dm", "analytics", "ecommerce", "dummyjson", "data-mart"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    # Ждем завершения ODS слоя
    sensor_on_ods_layer = ExternalTaskSensor(
        task_id="sensor_on_ods_layer",
        external_dag_id="ecommerce_ods_pg",
        #external_task_id="end",
        allowed_states=["success"],
        mode="reschedule",
        timeout=36000,
        poke_interval=60,
    )

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};
        """,
    )


    # 2. Создаем таблицу и заполняем данными
    create_dm = SQLExecuteQueryOperator(
        task_id="create_dm",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        -- 1. Создаем таблицу если не существует
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.simple_products (
            report_date DATE,
            category TEXT,
            product_count INTEGER,
            total_stock INTEGER,
            avg_price NUMERIC,
            avg_rating NUMERIC
        );

        -- 2. Удаляем данные за сегодня (если уже есть)
        DELETE FROM {TARGET_SCHEMA}.simple_products
        WHERE report_date = CURRENT_DATE;

        -- 3. Вставляем новые данные
        INSERT INTO {TARGET_SCHEMA}.simple_products
        SELECT
            CURRENT_DATE,
            category,
            COUNT(*) as product_count,
            SUM(stock) as total_stock,
            AVG(price) as avg_price,
            AVG(rating) as avg_rating
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        GROUP BY category;
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> sensor_on_ods_layer >> create_schema >> create_dm >> end