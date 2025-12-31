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



## 🔮 **Future Transformations** *(Planned)*

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

## 📊 **Sample Data Schema**

| Column | Type | Sample |
|--------|------|--------|
| `person_name` | String | mariataylor |
| `txn_amount` | Decimal | 56005.24 |
| `txn_status` | String | Declined |
| `merch_cat` | String | Grocery |

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