/*
===============================================================================
File Name   : gold.fraud_rate.sql
Project     : DataWarehouse - Gold Layer (Risk Metrics View)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer view 'gold.fraud_rate',
    which calculates daily fraud metrics based on transaction data.

    Purpose:
        - Measure fraud occurrence over time
        - Track fraud trends and anomalies
        - Support risk monitoring and fraud detection analysis

    Data Source:
        - gold.fact_transactions

    Key Transformations:
        - Aggregates transaction data at daily granularity
        - Calculates:
            * total_txns (total transactions)
            * fraud_txns (transactions flagged as fraud)
            * fraud_rate (fraud_txns / total_txns)
        - Converts fraud_flag to integer for aggregation

    Output Columns:
        - txn_date
        - total_txns
        - fraud_txns
        - fraud_rate

    Use Cases:
        - Fraud monitoring dashboards
        - Risk analysis and alerting
        - Trend analysis of fraudulent activity
        - KPI reporting for fraud detection teams

Notes:
        - This is a view (not a physical table), ensuring real-time calculation
        - Assumes fraud_flag is correctly populated in fact_transactions
        - Can be extended to segment fraud by user, merchant, or geography

Dependencies:
        - gold.fact_transactions must be available

Usage:
        SELECT * FROM gold.fraud_rate;

===============================================================================
*/

CREATE OR ALTER VIEW gold.fraud_rate
AS
SELECT
    CAST(txn_date AS DATE) AS txn_date,
    COUNT(*) AS total_txns,
    SUM(CAST(fraud_flag AS INT)) AS fraud_txns,
    SUM(CAST(fraud_flag AS INT)) * 1.0 / COUNT(*) AS fraud_rate
FROM gold.fact_transactions
GROUP BY CAST(txn_date AS DATE);
