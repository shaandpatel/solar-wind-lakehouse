# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.config import BRONZE_TABLE, SILVER_TABLE, QUARANTINE_TABLE

spark = SparkSession.builder.getOrCreate()

print(f"Starting Silver ETL & Data Quality checks on {BRONZE_TABLE}...")

# 1. Initialize Delta Tables via Spark SQL if they don't exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    time_tag TIMESTAMP,
    solar_wind_speed DOUBLE,
    proton_density DOUBLE,
    magnetic_field_total DOUBLE,
    magnetic_field_bz DOUBLE,
    plasma_temperature INTEGER,
    status_code STRING,
    _ingested_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
    time_tag TIMESTAMP,
    solar_wind_speed DOUBLE,
    proton_density DOUBLE,
    magnetic_field_total DOUBLE,
    magnetic_field_bz DOUBLE,
    plasma_temperature INTEGER,
    status_code STRING,
    quarantine_reason STRING,
    _quarantined_at TIMESTAMP
) USING DELTA
""")

# 2. Read Bronze Data
bronze_df = spark.table(BRONZE_TABLE)

# 3. Define Comprehensive Validation Rules

# Primary key checks
pk_valid = F.col("time_tag").isNotNull()

# Feature validity checks
features_valid = (
    F.col("solar_wind_speed").isNotNull() & 
    F.col("proton_density").isNotNull() & 
    F.col("magnetic_field_total").isNotNull() & 
    F.col("magnetic_field_bz").isNotNull() & 
    F.col("plasma_temperature").isNotNull() & 
    (F.col("solar_wind_speed") >= 0) &
    (F.col("status_code") == "OK")
)

# Combined master rule
valid_condition = pk_valid & features_valid

# Filter clean vs invalid data
valid_df = bronze_df.filter(valid_condition).dropDuplicates(["time_tag"])

# Tag invalid records with specific audit reasons
invalid_df = bronze_df.filter(~valid_condition) \
    .withColumn("quarantine_reason", 
        F.when(F.col("time_tag").isNull(), "Missing Primary Key (time_tag)")
         .when(F.col("solar_wind_speed").isNull(), "Null Solar Wind Speed Measurement")
         .when(F.col("proton_density").isNull(), "Null Proton Density Measurement")
         .when(F.col("magnetic_field_total").isNull(), "Null Magnetic Field Measurement")
         .when(F.col("magnetic_field_bz").isNull(), "Null Z Magnetic Field Measurement")
         .when(F.col("plasma_temperature").isNull(), "Null Plasma Temperature Measurement")
         .when(F.col("solar_wind_speed") < 0, "Negative Solar Wind Speed Value")
         .when(F.col("status_code") != "OK", "Non-OK Sensor Flag")
         .otherwise("Failed Validation Check")
    ) \
    .withColumn("_quarantined_at", F.current_timestamp())

# 4. Route Invalid Records to Quarantine Table
quarantine_count = invalid_df.count()
if quarantine_count > 0:
    invalid_df.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)
    print(f"Quarantined {quarantine_count} bad record(s) -> {QUARANTINE_TABLE}")
else:
    print("Zero data quality failures detected.")

# 5. Upsert (MERGE) Valid Data into Silver Table using Spark SQL
valid_df.createOrReplaceTempView("staged_updates")

spark.sql(f"""
    MERGE INTO {SILVER_TABLE} AS target
    USING staged_updates AS source
    ON target.time_tag = source.time_tag
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

silver_count = valid_df.count()
print(f"Successfully merged {silver_count} clean record(s) -> {SILVER_TABLE}")

