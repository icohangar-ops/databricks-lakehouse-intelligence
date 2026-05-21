# 🏔️ Databricks Lakehouse Intelligence Suite

**Medallion Architecture + Unity Catalog + MLflow on Databricks Free Edition**

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-Free%20Edition-orange" alt="Databricks Edition">
  <img src="https://img.shields.io/badge/Architecture-Medallion-blue" alt="Medallion">
  <img src="https://img.shields.io/badge/License-MIT-green" alt="License">
  <img src="https://img.shields.io/badge/Python-3.11+-yellow" alt="Python">
</p>

---

## 📋 Overview

The **Lakehouse Intelligence Suite** is a comprehensive mining & metals analytics platform built entirely on Databricks Community Edition (Free Tier). It demonstrates how to implement production-grade data engineering, ML experiment tracking, and SQL analytics without any paid infrastructure.

This project implements the **Medallion Architecture** (Bronze → Silver → Gold) using Unity Catalog for governance and MLflow for experiment tracking. It processes mining company data through quality stages to produce actionable intelligence scores.

### Key Capabilities

- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated) data pipeline
- **Unity Catalog**: 11 managed schemas under `workspace` catalog
- **MLflow Tracking**: Signal score experiment tracking with parameterized models
- **SQL Analytics**: Dashboard-ready SQL queries for Lakeview dashboards
- **Sample Data**: Realistic mining company operational & financial data

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATABRICKS WORKSPACE                          │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   BRONZE      │    │   SILVER     │    │    GOLD      │       │
│  │   (Raw)       │───▶│   (Cleaned)  │───▶│ (Aggregated) │       │
│  │               │    │              │    │              │       │
│  │ • Companies   │    │ • Deduped    │    │ • Signal     │       │
│  │ • Production  │    │ • Validated  │    │   Scores     │       │
│  │ • Financials  │    │ • Normalized │    │ • Rankings   │       │
│  └──────────────┘    └──────────────┘    └──────┬───────┘       │
│                                                   │               │
│                                                   ▼               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   MLFLOW     │    │   SQL DASH   │    │   LAKEVIEW   │       │
│  │   TRACKING   │◀───│   QUERIES    │───▶│   DASHBOARDS │       │
│  │              │    │              │    │              │       │
│  │ • Params     │    │ • Top Signals│    │ • KPI Cards  │       │
│  │ • Metrics    │    │ • Trends     │    │ • Charts     │       │
│  │ • Models     │    │ • Filters    │    │ • Tables     │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    UNITY CATALOG                           │   │
│  │  workspace.mining_bronze │ workspace.mining_silver │ ...  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Medallion Layers

| Layer | Schema | Purpose | Operations |
|-------|--------|---------|------------|
| **Bronze** | `workspace.mining_bronze` | Raw data ingestion | Append-only, no transforms |
| **Silver** | `workspace.mining_silver` | Cleaned & validated | Dedup, type cast, null handling |
| **Gold** | `workspace.mining_gold` | Business aggregates | Signal scores, rankings, KPIs |

---

## ⚡ Databricks Features Used

| Feature | Implementation | Status |
|---------|---------------|--------|
| **Unity Catalog** | 11 schemas under `workspace` catalog | ✅ Configured |
| **Managed Delta Tables** | Bronze, Silver, Gold layers | ✅ Active |
| **Medallion Architecture** | 3-tier data pipeline | ✅ Implemented |
| **MLflow Tracking** | Signal score experiments | ✅ Configured |
| **SQL Warehouses** | Dashboard queries | ✅ Available |
| **Lakeview Dashboards** | KPI & analytics dashboards | ✅ Ready |
| **Notebook Workflows** | Sequential pipeline execution | ✅ Linked |
| **DBSQL** | Ad-hoc SQL analytics | ✅ Enabled |
| **Job Scheduler** | Automated pipeline runs | ✅ Available |

---

## 🛠️ Workspace Setup

The Databricks workspace was configured via the **Workspace API**. Below are the key setup operations:

### Unity Catalog Schemas

```python
# 11 schemas created under workspace catalog
schemas = [
    "mining_bronze",
    "mining_silver",
    "mining_gold",
    "risk_bronze",
    "risk_silver",
    "risk_gold",
    "ml_experiments",
    "ml_models",
    "dashboards",
    "reporting",
    "staging"
]

for schema in schemas:
    requests.post(
        f"{BASE}/api/2.1/unity-catalog/schemas",
        headers=HEADERS,
        json={
            "name": schema,
            "catalog_name": "workspace",
            "comment": f"Schema for {schema.replace('_', ' ')}"
        }
    )
```

### Notebook Uploads

```python
# Upload notebooks to /Shared/Lakehouse_Intelligence/
for nb_name in notebooks:
    requests.post(
        f"{BASE}/api/2.0/workspace/import",
        headers=HEADERS,
        json={
            "path": f"/Shared/Lakehouse_Intelligence/{nb_name}",
            "content": base64_encoded_content,
            "language": "PYTHON",
            "format": "SOURCE",
            "overwrite": True
        }
    )
```

### API Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `POST /api/2.1/unity-catalog/schemas` | Create managed schemas |
| `POST /api/2.0/workspace/import` | Upload notebooks |
| `GET /api/2.0/workspace/export` | Export notebooks |
| `GET /api/2.0/workspace/list` | List workspace objects |

---

## 📓 Notebook Guide

| # | Notebook | Description | Key Operations |
|---|----------|-------------|----------------|
| 00 | `00_setup.py` | Environment setup & catalog configuration | Initialize Spark, verify Unity Catalog access |
| 01 | `01_bronze_ingest.py` | Raw data ingestion into Bronze layer | Create 3 Delta tables: companies, production, financials |
| 02 | `02_silver_transform.py` | Data cleaning & validation | Deduplication, type casting, null handling |
| 03 | `03_gold_aggregate.py` | Business intelligence aggregation | Compute 5-dimension signal scores (0-100) |
| 04 | `04_mlflow_experiments.py` | ML experiment tracking | Log signal score experiments with MLflow |
| 05 | `05_dashboard_sql.py` | SQL analytics for dashboards | Top signals, trends, rankings queries |

### Notebook Workflow

```
00_setup ──▶ 01_bronze_ingest ──▶ 02_silver_transform
                                     │
                                     ▼
                        03_gold_aggregate ──▶ 04_mlflow_experiments
                                     │
                                     ▼
                          05_dashboard_sql
```

---

## 📊 Sample Data

### Mining Companies (10 records)

| Company | Ticker | Country | Commodity | Grade (%) | Market Cap ($B) |
|---------|--------|---------|-----------|-----------|-----------------|
| BHP Group | BHP | Australia | Iron Ore | 58.2 | 148.5 |
| Rio Tinto | RIO | UK/Australia | Iron Ore | 62.1 | 112.3 |
| Glencore | GLEN | Switzerland | Copper/Ni | 1.8 | 62.1 |
| Anglo American | AAL | UK | PGMs/De Beers | 4.2 | 42.7 |
| Vale | VALE | Brazil | Iron Ore | 55.7 | 78.9 |
| First Quantum | FM | Canada | Copper | 0.45 | 12.8 |
| Teck Resources | TECK | Canada | Cu/Zn | 0.38 | 18.5 |
| Eramet | ERA | France | Ni/Mn | 1.2 | 6.8 |
| South32 | S32 | Australia | Al/Mn | 42.1 | 15.3 |
| Ivanhoe Mines | IVN | Canada | Cu/Pt/Pd | 2.8 | 22.1 |

### Production Records (15 records)
- Monthly production data across commodities
- Volume (tonnes), grade recovery (%), cost per unit

### Financial Metrics (10 records)
- Quarterly financials: revenue, EBITDA, capex, free cash flow
- Debt-to-equity, ROIC, dividend yield

---

## 🚀 Quick Start

### Prerequisites
- Databricks Community Edition account (free)
- Python 3.11+ with Databricks Connect (optional)

### Step 1: Clone & Upload

```bash
git clone https://github.com/Cubiczan/databricks-lakehouse-intelligence.git
cd databricks-lakehouse-intelligence
```

### Step 2: Upload to Databricks

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Upload notebooks
for nb in notebooks/*.py; do
    databricks workspace import "$nb" "/Shared/Lakehouse_Intelligence/$(basename $nb)" --language PYTHON --format SOURCE
done
```

### Step 3: Run the Pipeline

1. Open notebook `00_setup.py` in Databricks
2. Run sequentially: `00` → `01` → `02` → `03` → `04` → `05`
3. Each notebook creates/updates Delta tables in Unity Catalog

### Step 4: Create a Dashboard

1. Navigate to **Dashboards** → **Create Dashboard**
2. Use SQL queries from `05_dashboard_sql.py`
3. Add KPI cards, bar charts, and trend lines

---

## 🔧 Tech Stack

| Component | Technology |
|-----------|-----------|
| **Platform** | Databricks Community Edition |
| **Compute** | Serverless / Shared clusters |
| **Catalog** | Unity Catalog (workspace) |
| **Storage** | Delta Lake (managed tables) |
| **ML Tracking** | MLflow |
| **Language** | Python / PySpark / SQL |
| **Governance** | Unity Catalog schemas |
| **Visualization** | Lakeview Dashboards |
| **API** | Databricks REST API 2.0/2.1 |

### Python Package (src/lakehouse/)

```python
# Signal Score Computation
from src.lakehouse.signal_engine import SignalEngine

engine = SignalEngine()
score = engine.compute_signal(
    grade_score=85,
    cost_score=72,
    production_score=90,
    growth_score=65,
    esg_score=78
)
# → Weighted signal score: 0-100
```

### Dependencies

```toml
[project]
name = "databricks-lakehouse-intelligence"
requires-python = ">=3.11"
dependencies = [
    "databricks-sdk",
    "mlflow",
    "pandas",
    "pydantic>=2.0",
]
```

---

## 📁 Project Structure

```
databricks-lakehouse-intelligence/
├── README.md
├── pyproject.toml
├── .gitignore
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_aggregate.py
│   ├── 04_mlflow_experiments.py
│   └── 05_dashboard_sql.py
├── src/
│   └── lakehouse/
│       ├── __init__.py
│       ├── config.py
│       ├── models.py
│       ├── signal_engine.py
│       └── sql_queries.py
└── data/
    ├── sample_mining_companies.csv
    ├── sample_production.csv
    └── sample_financials.csv
```

---

## 🌐 Workspace

**Databricks Workspace**: [https://REDACTED_DATABRICKS_WORKSPACE](https://REDACTED_DATABRICKS_WORKSPACE)

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 👤 Author

**Shyam Desigan**
- Email: sam@cubiczan.com
- GitHub: [Cubiczan](https://github.com/Cubiczan)
- Specialization: Data Engineering, Mining Analytics, Cloud Architecture
