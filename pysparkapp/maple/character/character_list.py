from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import os

s3_access_key = os.environ['S3_ACCESS_KEY']
s3_secret_key = os.environ['S3_SECRET_KEY']

spark = (
    SparkSession.builder
    .appName("Character_list_to_DB")
    .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",s3_access_key)
    .config("spark.hadoop.fs.s3a.secret.key",s3_secret_key)
    .config('spark.hadoop.fs.s3a.path.style.access',"true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

raw_df = spark.read.json("s3a://maple-character-api/character/list/20260701_data.json")

raw_df.persist()

# raw_df.show(truncate=False)

result = raw_df.select(
        "account_id",
        explode("character_list").alias("character")
).select("account_id", "character.*")

raw_df.unpersist()

result.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://host.docker.internal:1433;databaseName=nexon;encrypt=true;trustServerCertificate=true") \
    .option("dbtable", "character_list") \
    .option("user", "user") \
    .option("password", "password") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("append") \
    .save()
