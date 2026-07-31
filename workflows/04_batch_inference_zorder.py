# Databricks notebook source
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from src.config import SILVER_TABLE, GOLD_TABLE, MLFLOW_EXPERIMENT_PATH

spark = SparkSession.builder.getOrCreate()
print(f"Starting Batch Inference on table {SILVER_TABLE}...")

# 1. Read Silver Table into Temp View
silver_df = spark.read.table(SILVER_TABLE)
silver_df.createOrReplaceTempView("silver_data")

features_df = spark.sql("""
    SELECT
        time_tag,

        -- Raw Features
        solar_wind_speed,
        proton_density,
        magnetic_field_total,
        magnetic_field_bz,
        plasma_temperature,

        -- Rolling Means
        COALESCE(AVG(solar_wind_speed) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), solar_wind_speed) AS rolling_5p_wind_speed,
        COALESCE(AVG(magnetic_field_total) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), magnetic_field_total) AS rolling_mag_mean,
        COALESCE(AVG(proton_density) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), proton_density) AS rolling_density_mean,
        COALESCE(AVG(plasma_temperature) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), plasma_temperature) AS rolling_temp_mean,
        COALESCE(AVG(magnetic_field_bz) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), magnetic_field_bz) AS rolling_bz_mean,

        -- Rolling Standard Deviations
        COALESCE(STDDEV(magnetic_field_total) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), 0.0) AS rolling_std_mag_field,
        COALESCE(STDDEV(solar_wind_speed) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), 0.0) AS rolling_std_wind_speed,

        -- Lag Features
        COALESCE(LAG(solar_wind_speed, 1) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag), solar_wind_speed) AS wind_lag_1,
        COALESCE(LAG(magnetic_field_bz, 1) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag), magnetic_field_bz) AS bz_lag_1,

        -- Change Features
        COALESCE(solar_wind_speed - LAG(solar_wind_speed, 1) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag), 0.0) AS wind_delta,
        COALESCE(magnetic_field_bz - LAG(magnetic_field_bz, 1) OVER (PARTITION BY DATE_TRUNC('month', time_tag) ORDER BY time_tag), 0.0) AS bz_delta

    FROM silver_data
""").dropna()

feature_cols = [
    "solar_wind_speed", "proton_density", "magnetic_field_total", "magnetic_field_bz", "plasma_temperature",
    "rolling_5p_wind_speed", "rolling_mag_mean", "rolling_density_mean", "rolling_temp_mean", "rolling_bz_mean",
    "rolling_std_mag_field", "rolling_std_wind_speed",
    "wind_lag_1", "bz_lag_1", "wind_delta", "bz_delta"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
assembled_df = assembler.transform(features_df)

# 2. Load Pipeline Model & Score Data
experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_PATH)

latest_run = mlflow.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["start_time DESC"],
    max_results=1
)

latest_run_id = latest_run.iloc[0].run_id
model_uri = f"runs:/{latest_run_id}/kmeans_pipeline"

print(f"Dynamically fetched latest MLflow Run ID: {latest_run_id}")
print(f"Loading Pipeline from: {model_uri}")

loaded_pipeline = mlflow.spark.load_model(model_uri)
scored_df = loaded_pipeline.transform(assembled_df)

final_gold_df = scored_df.select(
    "time_tag",
    "solar_wind_speed", "proton_density", "magnetic_field_total", "magnetic_field_bz", "plasma_temperature",
    "rolling_5p_wind_speed", "rolling_mag_mean", "rolling_density_mean", "rolling_temp_mean", "rolling_bz_mean",
    "rolling_std_mag_field", "rolling_std_wind_speed",
    "wind_lag_1", "bz_lag_1", "wind_delta", "bz_delta",
    "anomaly_cluster"
)

# 3. Save, Optimize, and Time Travel using Table Identifiers
final_gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
print(f"Batch inference saved to table: {GOLD_TABLE}")

print("Optimizing Gold Table for fast querying...")
spark.sql(f"OPTIMIZE {GOLD_TABLE} ZORDER BY (time_tag)")

print("Running Delta Time Travel Audit...")
history_df = spark.sql(f"DESCRIBE HISTORY {GOLD_TABLE}")

latest_version = history_df.select("version").head()[0]
if latest_version > 0:
    previous_version = latest_version - 1
    history_old = spark.read.option("versionAsOf", previous_version).table(GOLD_TABLE)
    print(f"Current Version ({latest_version}) Rows: {final_gold_df.count()}")
    print(f"Previous Version ({previous_version}) Rows: {history_old.count()}")
else:
    print("This is Version 0. No previous history to travel back to yet.")