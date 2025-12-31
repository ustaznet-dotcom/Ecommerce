import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor


# Конфигурация DAG
OWNER = "umar"
DAG_ID = "FCT_dm_ecommerce"

# Конфигурация таблиц
LAYER = "raw"                     # Слой, откуда берем данные
SOURCE = "ecommerce"              # Источник данных
SOURCE_SCHEMA = "ods"             # Откуда берем данные
SOURCE_TABLE = "fct_products"

TARGET_SCHEMA = "dm"  # Куда сохраняем витрины
TARGET_TABLE = "fct_products_stats"

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
        sql="""
    CREATE TABLE IF NOT EXISTS {{ params.target_schema }}.{{ params.target_table }} (
        report_date DATE,
        category TEXT,
        product_count INTEGER,
        total_stock INTEGER,
        avg_price NUMERIC,
        avg_rating NUMERIC
    );

    DELETE FROM {{ params.target_schema }}.{{ params.target_table }}
    WHERE report_date = '{{ ds }}';

    INSERT INTO {{ params.target_schema }}.{{ params.target_table }}
    SELECT
        '{{ ds }}'::DATE,
        category,
        COUNT(*),
        SUM(stock),
        AVG(price),
        AVG(rating)
    FROM {{ params.source_schema }}.{{ params.source_table }}
    GROUP BY category;
    """,
    params={
        "target_schema": TARGET_SCHEMA,
        "target_table": TARGET_TABLE,
        "source_schema": SOURCE_SCHEMA,
        "source_table": SOURCE_TABLE,
    },
)

    end = EmptyOperator(task_id="end")

    start >> sensor_on_ods_layer >> create_schema >> create_dm >> end