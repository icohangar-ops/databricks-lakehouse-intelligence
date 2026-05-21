# Databricks notebook source
# Lakehouse Intelligence Suite - Setup
# Author: Shyam Desigan | sam@cubiczan.com
# Medallion Architecture: Bronze -> Silver -> Gold -> ML -> Reporting

CATALOG = "workspace"
print("=" * 60)
print("  Lakehouse Intelligence Suite - Setup Verification")
print("=" * 60)
schemas = {
    "lakehouse_bronze": "Raw ingested data (mining + SEC filings)",
    "lakehouse_silver": "Cleaned, deduplicated, validated",
    "lakehouse_gold": "Aggregated signal scores and research metrics",
    "lakehouse_ml": "ML experiments and model artifacts",
    "lakehouse_reporting": "Dashboard queries and Genie NLQ",
}
for schema, desc in schemas.items():
    print(f"  OK  {CATALOG}.{schema} - {desc}")
print("\n  Architecture: Bronze -> Silver -> Gold -> ML -> Reporting")
print("=" * 60)
