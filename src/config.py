# Databricks notebook source
CATALOG = "hive_metastore" 
SCHEMA = "solar_wind_db"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_sw"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_sw"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.quarantine_sw"
GOLD_TRAINING_EVAL_TABLE = f"{CATALOG}.{SCHEMA}.gold_training_eval"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_anomalies"

RAW_DATA_PATH = "dbfs:/FileStore/tables/historicsw.csv"

MLFLOW_EXPERIMENT_PATH = "/Users/shaandpatel98@gmail.com/solar-wind-lakehouse"