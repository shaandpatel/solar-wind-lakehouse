# workflows/05_orchestrate_pipeline.py
import time


# Solar Wind Lakehouse Pipeline DAG Definition

# Order is strictly enforced: Bronze -> Silver -> Gold (Train) -> Gold (Infer)
pipeline_stages = [
    {
        "step": "Stage 1: Bronze Data Ingestion",
        "notebook": "./01_ingest_bronze",
        "timeout": 600  # 10 minutes max
    },
    {
        "step": "Stage 2: Silver Cleaning & Data Quarantine",
        "notebook": "./02_clean_silver_quarantine",
        "timeout": 600
    },
    {
        "step": "Stage 3: Gold Feature Engineering & MLflow Training",
        "notebook": "./03_gold_mlflow_train",
        "timeout": 1200 # 20 minutes max
    },
    {
        "step": "Stage 4: Gold Batch Inference & Delta Optimization",
        "notebook": "./04_batch_inference_zorder",
        "timeout": 900  # 15 minutes max
    }
]

# =========================================================================
# Execution Loop
# =========================================================================
print("==================================================")
print("Starting Solar Wind Lakehouse Pipeline")
print("==================================================\n")

pipeline_start_time = time.time()

for stage in pipeline_stages:
    step_name = stage["step"]
    notebook_path = stage["notebook"]
    timeout_secs = stage["timeout"]
    
    print(f"Executing {step_name}...")
    step_start_time = time.time()
    
    try:
        result = dbutils.notebook.run(notebook_path, timeout_secs)
        
        step_duration = round(time.time() - step_start_time, 2)
        print(f"{step_name} finished successfully in {step_duration}s.")
        print("-" * 50)
        
    except Exception as e:
        step_duration = round(time.time() - step_start_time, 2)
        print(f"\nFAILURE: {step_name} failed after {step_duration}s!")
        print(f"Error Details: {e}\n")
        
        # Halt execution to protect downstream Delta tables from corrupted or missing data
        raise RuntimeError(f"Pipeline Halted: Upstream task '{step_name}' failed.")

total_pipeline_duration = round(time.time() - pipeline_start_time, 2)

print("\n==================================================")
print(f"Solar Wind Lakehouse Pipeline executed end-to-end successfully!")
print(f"\nTotal Execution Time: {total_pipeline_duration} seconds")
print("==================================================")

