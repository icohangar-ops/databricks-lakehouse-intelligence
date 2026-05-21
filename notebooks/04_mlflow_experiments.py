# Databricks notebook source
# MLflow - Signal Score Experiment Tracking
import mlflow
from pyspark.sql.functions import *
CATALOG = "workspace"
print("MLflow - Experiment Tracking")

mlflow.set_experiment("/Lakehouse_Intelligence/signal_score_v1")
df = spark.read.table(f"{CATALOG}.lakehouse_gold.mining_signal_scores")

configs = [
    {"grade":0.20,"cost":0.25,"production":0.20,"growth":0.20,"esg":0.15,"name":"baseline"},
    {"grade":0.15,"cost":0.30,"production":0.25,"growth":0.15,"esg":0.15,"name":"cost_focused"},
    {"grade":0.25,"cost":0.15,"production":0.15,"growth":0.25,"esg":0.20,"name":"growth_focused"},
    {"grade":0.20,"cost":0.20,"production":0.20,"growth":0.15,"esg":0.25,"name":"esg_focused"},
]

for c in configs:
    with mlflow.start_run(run_name=c["name"]):
        mlflow.log_param("grade_weight",c["grade"])
        mlflow.log_param("cost_weight",c["cost"])
        mlflow.log_param("production_weight",c["production"])
        mlflow.log_param("growth_weight",c["growth"])
        mlflow.log_param("esg_weight",c["esg"])
        df_t = df.withColumn("test_score",round(col("grade_score")*c["grade"]+col("cost_score")*c["cost"]+col("production_score")*c["production"]+col("growth_score")*c["growth"]+col("esg_score")*c["esg"],1))
        avg_s = df_t.agg(avg("test_score")).collect()[0][0]
        mlflow.log_metric("avg_signal_score",round(avg_s,2))
        mlflow.log_metric("max_score",df_t.agg(max("test_score")).collect()[0][0])
        mlflow.log_metric("min_score",df_t.agg(min("test_score")).collect()[0][0])
        mlflow.log_metric("num_companies",df_t.count())
        print(f"  {c['name']}: avg={avg_s:.1f}")

print("MLflow experiments complete. View in sidebar.")
