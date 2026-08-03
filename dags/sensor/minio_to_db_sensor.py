from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.sdk import DAG,task
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum


conn_id ='maple-rdbms-mssql'

with DAG(
    dag_id='minio_to_db_sensor',
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=['sensor','pipeline_meta'],
) as dag:

    watcher = SqlSensor(
        task_id='pipeline_meta_watcher',
        conn_id=conn_id,
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

    @task
    def spark_parameter():
        hook = OdbcHook(
            odbc_conn_id=conn_id,
            driver="ODBC Driver 18 for SQL Server",
        )

        rows = hook.get_records(
            sql= """
                SELECT 
                    data_name,
                    target_path
                FROM dbo.pipeline_meta
                WHERE status = ?
            """,
            parameters= ("READY",),
        )

        return [
            {
                "data_name" : rows[0].replace('/','_')
                "target_path" : rows[1]
            }
        ]


    spark_parameter=spark_parameter()

    watcher >> spark_parameter
