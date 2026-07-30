# Raw Data Source
RAW_DATA_PATH = "/Workspace/Users/shaandpatel98@gmail.com/historicsw.csv"  # or file:/tmp/historicsw.csv

# Local Path-Based Delta Storage
BRONZE_PATH = "file:/tmp/swdb/bronze_sw"
SILVER_PATH = "file:/tmp/swdb/silver_sw"
QUARANTINE_PATH = "file:/tmp/swdb/quarantine_sw"
GOLD_TRAINING_EVAL_PATH = "file:/tmp/swdb/gold_training_eval"
GOLD_PATH = "file:/tmp/swdb/gold_anomalies"

# MLflow Experiment Configuration
MLFLOW_EXPERIMENT_PATH = "/Users/shaandpatel98@gmail.com/solar-wind-lakehouse"