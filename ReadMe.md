# 🚀 End-to-End Data Warehouse (Medallion Architecture)

## 📌 Overview

This project demonstrates the design and implementation of a **production-style data warehouse** using the **Medallion Architecture (Bronze → Silver → Gold)**.

It simulates a **fintech data platform** processing transactions, users, merchants, and fraud signals, transforming raw data into **analytics-ready datasets** for business insights.

---

## 🏗️ Architecture

The warehouse follows a **layered approach**:

```
Bronze Layer  → Raw data ingestion (as-is from source systems)
Silver Layer  → Cleaned, standardized, and enriched data
Gold Layer    → Business-ready data models for analytics
```

---

## 🔄 Data Pipeline Flow

1. **Data Sources**

   * CRM System (Users, Activity)
   * ERP System (Transactions, Fees, Merchants, Refunds, Fraud Signals)

2. **Bronze Layer**

   * Raw ingestion with minimal validation
   * Stores source data in original format

3. **Silver Layer**

   * Data cleaning and type standardization
   * Adds audit columns (`dwh_create_time`)
   * Prepares structured datasets

4. **Gold Layer**

   * Fact and dimension modeling
   * Aggregations and KPI tables
   * Business-ready datasets

5. **Consumption Layer**

   * BI dashboards
   * Reports
   * Ad-hoc analytics

---

## 📊 Data Model

### 🔹 Fact Tables

* `fact_transaction`

### 🔹 Dimension Tables

* `dim_users`
* `dim_merchants`

### 🔹 Analytical Tables

* `revenue_summary`
* `user_ltv`
* `retention_cohort`
* `funnel_analysis`
* `user_activity_metrics`
* `user_activity_monthly`
* `arpu`
* `churn_rate`
* `fraud_rate`
* `customer_segmentation`
* `fraud_anomalies`

---

## ⚙️ Key Features

* ✅ **Medallion Architecture (Bronze–Silver–Gold)**
* ✅ **Idempotent SQL Scripts** (safe re-runs)
* ✅ **Transaction-safe execution**
* ✅ **Data type standardization (NVARCHAR → DECIMAL, INT, DATETIME2)**
* ✅ **Audit & lineage tracking (`dwh_create_time`)**
* ✅ **Modular SQL structure for scalability**
* ✅ **Production-style schema design**

---

## 📈 Business Use Cases

* Customer Lifetime Value (LTV)
* Retention & Cohort Analysis
* Revenue & ARPU Tracking
* Funnel Analysis
* Fraud Detection & Anomaly Identification
* Customer Segmentation

---

## 🧪 Data Volume (Simulated)

* 👤 120,000+ Users
* 💳 400,000+ Transactions
* 🏬 50,000+ Merchants

---

## 🛠️ Tech Stack

* **SQL Server**
* **T-SQL**
* Data Warehousing Concepts
* ETL / ELT Design
* Data Modeling

---

## 📂 Project Structure

```
/scripts
│
├── 01_create_database_schema.sql
├── 02_create_bronze_tables.sql
├── 03_insert_data_bronze_tables.sql
├── 04_create_silver_tables.sql
├── 05_insert_data_silver_tables.sql
│
├── 06_gold.fact_transaction.sql
├── 07_gold.dim_merchants.sql
├── 08_gold.dim_users.sql
├── 09_gold.revenue_summary.sql
├── 10_gold.user_ltv.sql
├── 11_gold.retention_cohort.sql
├── 12_gold.funnel_analysis.sql
├── 13_gold.user_activity_metrics.sql
├── 14_gold.user_activity_monthly.sql
├── 15_gold.arpu.sql
├── 16_gold.churn_rate.sql
├── 17_gold.fraud_rate.sql
├── 18_gold.customer_segmentation.sql
├── 19_gold.fraud_anomalies.sql
```

---

## ▶️ How to Run

1. Create Database & Schemas:

   ```
   Run: 01_create_database_schema.sql
   ```

2. Create Bronze Tables:

   ```
   Run: 02_create_bronze_tables.sql
   ```

3. Load Bronze Data:

   ```
   Run: 03_insert_data_bronze_tables.sql
   ```

4. Create Silver Tables:

   ```
   Run: 04_create_silver_tables.sql
   ```

5. Load Silver Data:

   ```
   Run: 05_insert_data_silver_tables.sql
   ```

6. Build Gold Layer:

   ```
   Run scripts 06 → 19 in order
   ```

---

## 🧠 Design Decisions

* **Bronze Layer:** Keeps raw data untouched for traceability
* **Silver Layer:** Handles cleaning, casting, and standardization
* **Gold Layer:** Optimized for business queries and analytics
* **Idempotency:** Ensures safe re-execution of pipelines
* **Audit Columns:** Enables lineage tracking and debugging

---

## 🔮 Future Improvements

* ⏳ Add **incremental loading (MERGE / CDC)**
* ⚙️ Integrate **Azure Data Factory / Airflow for orchestration**
* 🧪 Implement **data quality tests**
* 📊 Build **Power BI dashboards**
* 🚀 Add **CI/CD pipeline for deployment**

---

## 🙌 Acknowledgements

This project is inspired by real-world **data engineering practices** used in modern data platforms.

---

## 📬 Connect

If you found this useful or have suggestions, feel free to connect or reach out!

---
