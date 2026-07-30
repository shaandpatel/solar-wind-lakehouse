# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.config import RAW_DATA_PATH, BRONZE_PATH
from src.schemas import solar_wind_schema

spark = SparkSession.builder.getOrCreate()

# 1. Read Raw Files using explicit PySpark StructType schema
raw_df = spark.read \
    .option("header", "true") \
    .schema(solar_wind_schema) \
    .csv(RAW_DATA_PATH)

# 2. Add Lineage Metadata
bronze_df = raw_df.withColumn("_ingested_at", F.current_timestamp())

# 3. Write to Bronze Delta Storage Path
bronze_df.write.format("delta").mode("overwrite").save(BRONZE_PATH)

print(f"Successfully written Bronze layer to path: {BRONZE_PATH}")