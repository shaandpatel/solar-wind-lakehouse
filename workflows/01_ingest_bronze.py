# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.config import RAW_DATA_PATH, BRONZE_TABLE, CATALOG, SCHEMA
from src.schemas import solar_wind_schema

spark = SparkSession.builder.getOrCreate()

# 1. Create the SQL Database/Schema in Databricks if it doesn't exist yet
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}")

# 2. Read Raw Files using explicit PySpark StructType schema
raw_df = spark.read \
    .option("header", "true") \
    .schema(solar_wind_schema) \
    .csv(RAW_DATA_PATH)

# 3. Add Lineage Metadata
bronze_df = raw_df.withColumn("_ingested_at", F.current_timestamp())

# 4. Write to Bronze Delta Table
bronze_df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

print(f"Successfully created database and written to Bronze: {BRONZE_TABLE}")