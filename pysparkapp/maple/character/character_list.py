from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,from_json
from pyspark.sql.types import  IntegerType,StringType,StructType,StructField,ArrayType
import os
import logging

s3_access_key = os.environ['S3_ACCESS_KEY']
s3_secret_key = os.environ['S3_SECRET_KEY']

schema= StructType(
    [
        StructField("account_id", IntegerType()),
        StructField("character_list", ArrayType(StringType(),True),True),
    ]
)


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

print(raw_df)


# parsed_df = raw_df.select(
#     col("id"),
#     col("name"),
#     col("created_at")
# )
#
# parsed_df.write \
#     .format("jdbc") \
#     .option("url", "jdbc:mysql://my-db:3306/mydb") \
#     .option("driver", "com.mysql.cj.jdbc.Driver") \
#     .option("dbtable", "parsed_api_data") \
#     .option("user", "user") \
#     .option("password", "password") \
#     .mode("append") \
#     .save()