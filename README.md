# Credit Card ETL Analytics Pipeline 🚀

[![PySpark](https://img.shields.io/badge/PySpark-3.5.3-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://docs.docker.com/compose/)
[![Python](https://img.shields.io/badge/Python-3.8+-green.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-brightgreen.svg)](LICENSE)

A **production-ready local ETL pipeline** using **PySpark + Docker Compose** for credit card transaction processing and analytics.

## 🎯 **Project Overview**

Raw Transactions → Spark Processing → 10 Analytics Sheets → Single Excel Report
↓ ↓ ↓
user/card/merch 10 Business KPIs Executive Dashboard
CSV files Fraud/Risk/Revenue /data/processed/analytics.xlsx

## 🚀 **Quick Start**

Clone & setup
- `git clone <repo>`
- `cd etl-platform`
- `mkdir -p data/{raw,processed}`

Start pipeline
- `docker compose up --build`
  
Background mode (detached)
- `docker compose up --build -d`

Specific services only

- `docker compose up postgres spark-master -d`
- `docker compose up transformation-app -d`

## Monitor

Live logs (ALL services)
- `docker compose logs -f`

Specific service logs
- `docker compose logs -f transformation-app`
- `docker compose logs -f spark-master`

Service status
- `docker compose ps`

## Full restart (clean slate)
- `docker compose down -v`
- `docker compose up --build`

Restart single service
- `docker compose restart transformation-app`

Rebuild single service
- `docker compose build transformation`
- `docker compose up transformation-app -d`

## Data & Output Verification
Check Excel output
- `ls -la data/processed/*.xlsx`

Postgres job status
- `docker compose exec postgres psql -U postgres -d etl_db -c "SELECT * FROM job_metadata"`

Raw data
- `ls -la data/raw/`

Spark jobs
- `curl http://localhost:8080/api/v1/applications`



# Spark UI
# http://localhost:8080



## 📁 **Directory Structure**
```
etl-platform/
├── docker-compose.yml # Orchestration
├── data/ # Input/Output
│ ├── raw/ # Generated CSVs
│ └── processed/ # 🚀 Analytics Excel
├── db/
│ └── init.sql # Schema
└── apps/
├── extraction/ # Raw data generation
└── transformation/ # 10 Analytics
```



## 🔮 **Transformations**

| # | **Transformation** | **Business Value** |
|---|--------------------|-------------------|
| 1 | High-Risk Txns | Fraud alerts (>₹50K Declined) |
| 2 | Merchant Performance | Revenue/approval rates |
| 3 | Customer Segments | Premium/Regular profiling |
| 4 | Fraud Alerts | >3 declines/day |
| 5 | Category Revenue | City-wise P&L |
| 6 | Reversal Impact | High-risk customers |
| 7 | Bill Cycle Anomalies | Unusual patterns |
| 8 | Payment Method | UPI/CC efficiency |
| 9 | Geographic Risk | City risk scores |
| 10 | Card Company | VISA/Maestro rankings |


## 🔮 Future Enhancements

- **Airflow Orchestration**  
  Use Apache Airflow to orchestrate the full workflow (extraction → transformation → Excel export), with retries, SLAs, and monitoring.

- **Kubernetes-native Spark**  
  Run Spark on Kubernetes (EKS/GKE/AKS or local K8s) with driver/executor pods for autoscaling and better resource isolation.

- **S3-based Data Lake**  
  Replace local `/data` with S3 (or MinIO) as the central storage layer for raw, processed, and analytics outputs.

- **Lakehouse & Table Formats**  
  Introduce Delta Lake or Apache Iceberg on S3 for ACID tables, schema evolution, and time travel.

- **Data Quality & Governance**  
  Add validation checks (Great Expectations / custom PySpark) and store results as data quality metrics.

- **Monitoring & Observability**  
  Integrate Prometheus + Grafana for Spark and Airflow metrics, plus alerting on failures or anomalies.

- **Incremental ETL**  
  Support partitioned, incremental loads (by `txn_date`/`bill_cyc`) instead of full reloads.

- **APIs & BI Integration**  
  Expose curated datasets via REST APIs and/or load into a warehouse for BI dashboards.

- **Streaming Extension (Long-Term)**  
  Extend to near real-time using Kafka + Spark Structured Streaming for fraud and risk alerts.



## 📊 **Sample Data Schema**

| Column | Type | Precision/Scale | Max Length | Sample |
|--------|------|-----------------|------------|--------|
| `person_name` | STRING | - | 50 | `mariataylor` |
| `acct_num` | STRING | - | 30 | `GB46COEO29222669332973` |
| `card_company` | STRING | - | 20 | `VISA` |
| `card_number` | STRING | - | 25 | `6583937500721886` |
| `txn_datetime` | STRING | `yyyy-MM-dd HH:mm:ss` | 30 | `2025-08-18 21:56:06` |
| `merch_name` | STRING | - | 50 | `Lenskart` |
| `merch_cat` | STRING | - | 40 | `Grocery` |
| `txn_amount` | DECIMAL | 12,2 | - | `56005.24` |
| `currency` | STRING | - | 10 | `INR` |
| `pay_meth` | STRING | - | 20 | `Credit Card` |
| `city` | STRING | - | 50 | `Nicolehaven` |
| `txn_type` | STRING | - | 20 | `Purchase` |
| `bill_cyc` | STRING | - | 20 | `March` |
| `txn_status` | STRING | - | 15 | `Reversed` |

## **Key Schema Notes** 🔍

- **`txn_amount`**: DECIMAL(12,2) → ₹999,999,999.99 maximum
- **`txn_datetime`**: String format `yyyy-MM-dd HH:mm:ss`  
  *Parse with Spark: `to_timestamp(col("txn_datetime"), "yyyy-MM-dd HH:mm:ss")`*
- **All STRING fields**: Length-constrained for database storage
- **Primary dimensions**: `person_name`, `card_company`, `merch_cat`, `city`, `pay_meth`

**Ready for Spark schema enforcement, data validation, and production pipelines!** 🎯


## 🛠️ **Tech Stack**
- Orchestration: Docker Compose | Airflow ( sooner )
- Processing: Apache Spark 3.5.3 + PySpark
- Database: PostgreSQL 15
- Analytics: Pandas + xlsxwriter
- Data Gen: Faker
- Output: Multi-sheet Excel (.xlsx)

## 📝 **License**
MIT License - Free for commercial use

---

**From raw transactions → Executive Excel dashboard in 5 minutes!** 🎉

⭐ **Star this repo** | 🐛 **Report bugs** | 💪 **Contribute**