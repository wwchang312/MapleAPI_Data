from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode
import os
import argparse
import pyodbc

s3_access_key = os.environ['S3_ACCESS_KEY']
s3_secret_key = os.environ['S3_SECRET_KEY']
db_url= os.environ['DB_URL']
db_usr = os.environ['MSSQL_USER']
db_pwd = os.environ['MSSQL_PASSWORD']
jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

#spark-submit시 받은 파라미터 파싱
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline-id', required=True, type=str)

    return parser.parse_args()

#Spark Session 생성
def create_spark_session() -> SparkSession:
    return(
        SparkSession.builder
        .appName("json_loader")
        .getOrCreate()
    )


# Pipeline 메타 테이블 read
def read_pipeline_meta(
        spark: SparkSession,
        pipeline_id: str,
        ):
    query = f"""
        SELECT 
            uuid,
            REPLACE(data_name,'/','_') as data_name,
            date_info,
            source_path
        FROM dbo.pipeline_meta
        WHERE uuid = '{pipeline_id}'
    """

    meta_df = (
        spark.read
        .format("jdbc")
        .option("url", db_url)
        .option("driver", jdbc_driver)
        .option("query", query)
        .option("user",db_usr)
        .option("password",db_pwd)
        .load()
    )

    row = meta_df.first()

    if row is None:
        raise ValueError(
            f"pipeline_id = '{pipeline_id}'에 해당하는 메타정보가 없습니다."
        )

    return row

#Minio에서 json 데이터 read

def read_json(spark: SparkSession, source_path: str):
    return spark.read.json(f's3a://{source_path}')


def parsing_json(df : DataFrame, data_nm:str):

    if data_nm == "character_list":
        raw_data=(
            df.select(
                "account_id",
                explode("character_list").alias("character")
            ).select("account_id","character.*")
        )

    else :
        raise ValueError(f"{data_nm}은 존재하지 않는 데이터입니다.")

    return raw_data

def write_to_stg_table(
        df: DataFrame,
        table_nm: str,
):
    (
    df.write
    .format("jdbc")
    .option("url", db_url)
    .option("dbtable", f"stg.stg_{table_nm}")
    .option("user", db_usr)
    .option("password", db_pwd)
    .option("driver", jdbc_driver)
    .mode("append")
    .save()
    )

def change_meta_status(
        pipeline_id: str,
        status: str,
        ):
    connections_info = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=mssql,1433;"
        "DATABASE=pipeline_db;"
        f"UID={db_usr};"
        f"PWD={{{db_pwd}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    connection = pyodbc.connect(connections_info)

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            UPDATE dbo.pipeline_meta
            SET status = '?',
                updated_at = GETDATE()
            WHERE pipeline_id ='?'
            """,
            status,
            pipeline_id,
        )

        if cursor.rowcount != 1:
            raise ValueError(
                f"오류가 발생했습니다."
            )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

def main():
    args = parse_args()

    spark = create_spark_session()

    try:
        meta = read_pipeline_meta(
            spark=spark,
            pipeline_id=args.pipeline_id,
        )

        source_df = read_json(
            spark=spark,
            source_path=meta["source_path"],
        )

        result_df = parsing_json(
            df=source_df,
            data_nm=meta["data_name"],
        )

        write_to_stg_table(
            df=result_df,
            table_nm=meta["data_name"],
        )

        change_meta_status(
            pipeline_id=args.pipeline_id,
            status="STAGING"
        )

    finally:
        spark.stop()

if __name__ == "__main__":
    main()




