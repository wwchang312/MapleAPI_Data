from airflow.providers.odbc.hooks.odbc import OdbcHook
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
            WHERE status = ?
        """,
        parameters= ("READY",),
        poke_interval=60,
        timeout= 600,
        mode='reschedule',
        hook_params={
            "driver" : "ODBC Driver 18 for SQL Server"
        },
    )

    test_task = EmptyOperator(
        task_id='test_task',
    )

    watcher >> test_task
