from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.sdk import DAG,task
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum

conn_id ='maple-rdbms-mssql'

with DAG(
    dag_id='migrate_data_stg_to_target',
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=['sensor','pipeline_meta'],
) as dag:

    # Sensor를 통한 pipeline_meta 테이블 감시
    watcher = SqlSensor(
        task_id='pipeline_meta_watcher',
        conn_id=conn_id,
        sql="""
                SELECT COUNT(*)
                FROM dbo.pipeline_meta
                WHERE status = ?
            """,
        parameters=("STAGING",),
        poke_interval=60,
        timeout=600,
        mode='reschedule',
        hook_params={
            "driver": "ODBC Driver 18 for SQL Server"
        },
    )


    @task
    def processing_staging_data():
        hook = OdbcHook(
            odbc_conn_id=conn_id,
            driver="ODBC Driver 18 for SQL Server",
            database="nexon",
        )

        sql = """
            UPDATE TOP (1) dbo.pipeline_meta
            SET
                status = 'PROCESSING',
                update_date = GETDATE()
            OUTPUT
                inserted.uuid
            WHERE status = 'STAGING';
        """

        row = hook.get_first(sql)

        if row is None:
            raise ValueError("처리할 STAGING 데이터가 없습니다.")

        return str(row[0])


    @task
    def load_staging_data(uuid: str):
        hook = OdbcHook(
            odbc_conn_id=conn_id,
            driver="ODBC Driver 18 for SQL Server",
            database="nexon",
        )

        sql = """
            EXEC maple.SP_LOAD_STAGING_DATA
                @uuid = ?;
        """

        hook.run(
            sql=sql,
            parameters=(uuid,),
        )


    processing_uuid  = processing_staging_data()
    load_task = load_staging_data(processing_uuid)

    watcher  >>  processing_uuid >> load_task