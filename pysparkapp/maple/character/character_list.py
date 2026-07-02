from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import  IntegerType,StringType,StructType,StructField,ArrayType
import os
import logging

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

raw_df = spark.read.json("s3a://maple-character-api/character/list/20260629/data.json")

raw_df.persist()

result = raw_df.select(
        "account_id",
        explode("character_list")
).select("account_id", "character_list.*")

raw_df.unpersist()

print(result)

