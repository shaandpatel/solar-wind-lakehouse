# Raw Data Source
RAW_DATA_PATH = "/Volumes/workspace/default/raw_data/historicsw.csv" 

# UC Delta Tables (catalog.schema.table)
BRONZE_TABLE = "workspace.default.sw_bronze"
SILVER_TABLE = "workspace.default.sw_silver"
QUARANTINE_TABLE = "workspace.default.sw_quarantine"
GOLD_TRAINING_EVAL_TABLE = "workspace.default.sw_gold_training_eval"
GOLD_TABLE = "workspace.default.sw_gold_anomalies"

# MLflow Experiment Configuration
MLFLOW_EXPERIMENT_PATH = "/Users/shaandpatel98@gmail.com/solar-wind-lakehouse"