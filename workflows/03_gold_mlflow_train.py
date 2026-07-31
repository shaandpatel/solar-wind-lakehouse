# Databricks notebook source
import mlflow
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from src.config import SILVER_TABLE, GOLD_TRAINING_EVAL_TABLE, MLFLOW_EXPERIMENT_PATH

spark = SparkSession.builder.appName("Solar_Wind_Lakehouse").getOrCreate()
print(f"Starting Gold Layer ML Workflow using input table: {SILVER_TABLE}...")

# 1. Read Silver Delta Table directly
silver_df = spark.read.table(SILVER_TABLE)
silver_df.createOrReplaceTempView("silver_data")

# 2. Feature Engineering via Spark SQL
gold_features_df = spark.sql("""
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

# 3. Features for VectorAssembler
feature_cols = [
    "solar_wind_speed", "proton_density", "magnetic_field_total", "magnetic_field_bz", "plasma_temperature",
    "rolling_5p_wind_speed", "rolling_mag_mean", "rolling_density_mean", "rolling_temp_mean", "rolling_bz_mean",
    "rolling_std_mag_field", "rolling_std_wind_speed",
    "wind_lag_1", "bz_lag_1", "wind_delta", "bz_delta"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
assembled_df = assembler.transform(gold_features_df)

# 4. MLflow Experiment Tracking & Pipeline Training
mlflow.set_experiment(MLFLOW_EXPERIMENT_PATH) 

with mlflow.start_run(run_name="kmeans_solar_anomaly") as run:
    k_clusters = 3
    seed = 25
    
    mlflow.log_param("k_clusters", k_clusters)
    mlflow.log_param("seed", seed)
    mlflow.log_param("feature_columns", feature_cols)
    
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    kmeans = KMeans(k=k_clusters, seed=seed, featuresCol="features", predictionCol="anomaly_cluster")
    
    pipeline = Pipeline(stages=[scaler, kmeans])
    pipeline_model = pipeline.fit(assembled_df)
    predictions_df = pipeline_model.transform(assembled_df)
    
    evaluator = ClusteringEvaluator(predictionCol="anomaly_cluster", featuresCol="features", metricName="silhouette")
    silhouette = evaluator.evaluate(predictions_df)
    
    mlflow.log_metric("silhouette_score", silhouette)
    mlflow.spark.log_model(pipeline_model, "kmeans_pipeline")
    
    print(f"Model Silhouette Score: {silhouette:.4f} logged to MLflow.")

# 5. Save Training Evaluation Data to UC Table
final_eval_df = predictions_df.select(
    "time_tag", "solar_wind_speed", "proton_density", "magnetic_field_total", "magnetic_field_bz", "plasma_temperature",
    "rolling_5p_wind_speed", "rolling_mag_mean", "rolling_density_mean", "rolling_temp_mean", "rolling_bz_mean",
    "rolling_std_mag_field", "rolling_std_wind_speed", "wind_lag_1", "bz_lag_1", "wind_delta", "bz_delta",
    "anomaly_cluster"
)

final_eval_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TRAINING_EVAL_TABLE)
print(f"Successfully generated anomaly tags and saved to table: {GOLD_TRAINING_EVAL_TABLE}")