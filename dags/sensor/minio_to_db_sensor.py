from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.sdk import DAG,task
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
import os

conn_id ='maple-rdbms-mssql'

with DAG(
    dag_id='minio_to_db_sensor',
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=['sensor','pipeline_meta'],
) as dag:

    #Sensor를 통한 pipeline_meta 테이블 감시
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

    #메타 테이블로부터 적재 대상 데이터 식별
    @task
    def spark_parameter():
        hook = OdbcHook(
            odbc_conn_id=conn_id,
            driver="ODBC Driver 18 for SQL Server",
        )

        rows = hook.get_records(
            sql= """
                SELECT 
                    uuid
                FROM dbo.pipeline_meta
                WHERE status = ?
            """,
            parameters= ("READY",),
        )

        return [
            ["--pipeline-id",str(row[0])]
            for row in rows
        ]

    pipeline_id = spark_parameter()

    #Spark job 제출
    submit_spark_jobs = SparkSubmitOperator.partial(
        task_id='pipeline_submit_spark_jobs',
        conn_id="spark-conn-id",
        application="pysparkapp/maple/character/stg_data.py",
        jars="/opt/spark/jars/mssql-jdbc-12.6.1.jre11.jar",
        conf={
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "/src/spark_venv/bin/python",
            "spark.executorEnv.PYSPARK_PYTHON": "/src/spark_venv/bin/python",
            "spark.yarn.appMasterEnv.S3_ACCESS_KEY": os.environ["S3_ACCESS_KEY"],
            "spark.yarn.appMasterEnv.S3_SECRET_KEY": os.environ["S3_SECRET_KEY"],
            "spark.yarn.appMasterEnv.DB_URL": os.environ["DB_URL"],
            "spark.yarn.appMasterEnv.MSSQL_USER": os.environ["MSSQL_USER"],
            "spark.yarn.appMasterEnv.MSSQL_PASSWORD": os.environ["MSSQL_PASSWORD"],
            "spark.executorEnv.S3_ACCESS_KEY": os.environ["S3_ACCESS_KEY"],
            "spark.executorEnv.S3_SECRET_KEY": os.environ["S3_SECRET_KEY"],
            "spark.executorEnv.DB_URL": os.environ["DB_URL"],
            "spark.executorEnv.MSSQL_USER": os.environ["MSSQL_USER"],
            "spark.executorEnv.MSSQL_PASSWORD": os.environ["MSSQL_PASSWORD"],
        },
        yarn_track_via_rm_api=False,
        reconnect_on_retry=False,
        verbose=True,
    ).expand(
        application_args=pipeline_id,
    )