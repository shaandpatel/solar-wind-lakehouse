# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.config import RAW_DATA_PATH, BRONZE_TABLE
from src.schemas import solar_wind_schema

spark = SparkSession.builder.getOrCreate()

# 1. Read Raw Files using explicit PySpark StructType schema
raw_df = spark.read.option("header", "true").schema(solar_wind_schema).csv(RAW_DATA_PATH)

# 2. Add Lineage Metadata
bronze_df = raw_df.withColumn("_ingested_at", F.current_timestamp())

# 3. Writes managed Delta tables directly inside Unity Catalog
bronze_df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)

print(f"Successfully written Bronze layer to table: {BRONZE_TABLE}")