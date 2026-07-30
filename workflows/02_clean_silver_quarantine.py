# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from src.config import BRONZE_PATH, SILVER_PATH, QUARANTINE_PATH

spark = SparkSession.builder.getOrCreate()

print(f"Starting Silver ETL & Data Quality checks on path {BRONZE_PATH}...")

# 1. Reads directly from UC table
bronze_df = spark.read.table(BRONZE_TABLE)

# 2. Define Comprehensive Validation Rules
pk_valid = F.col("time_tag").isNotNull()

features_valid = (
    F.col("solar_wind_speed").isNotNull() & 
    F.col("proton_density").isNotNull() & 
    F.col("magnetic_field_total").isNotNull() & 
    F.col("magnetic_field_bz").isNotNull() & 
    F.col("plasma_temperature").isNotNull() & 
    (F.col("solar_wind_speed") >= 0) &
    (F.col("status_code") == "OK")
)

valid_condition = pk_valid & features_valid

# Filter clean vs invalid data
valid_df = bronze_df.filter(valid_condition).dropDuplicates(["time_tag"])

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

# 3. Route Invalid Records to Quarantine Path
quarantine_count = invalid_df.count()
if quarantine_count > 0:
    invalid_df.write.format("delta").mode("append").save(QUARANTINE_PATH)
    print(f"Quarantined {quarantine_count} bad record(s) -> {QUARANTINE_PATH}")
else:
    print("Zero data quality failures detected.")

# 4. Upsert (MERGE) Valid Data into Silver Path
if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
    valid_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
else:
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    silver_table.alias("target").merge(
        valid_df.alias("source"),
        "target.time_tag = source.time_tag"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

silver_count = valid_df.count()
print(f"Successfully merged {silver_count} clean record(s) -> {SILVER_PATH}")