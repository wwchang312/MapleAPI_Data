from airflow.sdk import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum

with DAG(
    dag_id='minio_to_db_sensor',
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=['sensor','pipeline_meta'],
) as dag:

    watcher = SqlSensor(
        task_id='pipeline_meta_watcher',
        conn_id='maple-rdbms-mssql',
        sql = """
            SELECT COUNT(*)
            FROM dbo.pipeline_meta
            WHERE status = %(status)
        """,
        params={'status':"READY"},
        poke_interval=60,
        timeout= 600,
        mode='reschedule',
    )

    test_task = EmptyOperator(
        task_id='test_task',
    )

    watcher >> test_task
