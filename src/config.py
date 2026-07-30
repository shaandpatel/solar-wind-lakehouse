# Raw Data Source
RAW_DATA_PATH = "/Workspace/Users/shaandpatel98@gmail.com/historicsw.csv" 

# DBFS-Based Delta Storage
BRONZE_PATH = "dbfs:/tmp/swdb/bronze_sw"
SILVER_PATH = "dbfs:/tmp/swdb/silver_sw"
QUARANTINE_PATH = "dbfs:/tmp/swdb/quarantine_sw"
GOLD_TRAINING_EVAL_PATH = "dbfs:/tmp/swdb/gold_training_eval"
GOLD_PATH = "dbfs:/tmp/swdb/gold_anomalies"

# MLflow Experiment Configuration
MLFLOW_EXPERIMENT_PATH = "/Users/shaandpatel98@gmail.com/solar-wind-lakehouse"