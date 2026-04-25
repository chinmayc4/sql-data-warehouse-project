/*
===============================================================================
File Name   : gold_fact_transactions.sql
Project     : DataWarehouse - Gold Layer (Fact Table)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer fact view 'gold.fact_transactions',
    which provides a consolidated, analytics-ready dataset for transaction
    analysis by combining data from multiple Silver layer tables.

    Purpose:
        - Serve as a central fact table for reporting and analytics
        - Enable business-level metrics such as revenue, refunds, and fraud analysis
        - Simplify querying by pre-joining multiple transactional datasets

    Data Sources:
        - silver.erp_transactions (base transaction data)
        - silver.erp_fees (transaction fees)
        - silver.erp_refunds (refund details)
        - silver.erp_fraud_signals (risk and fraud indicators)

    Key Transformations:
        - Joins multiple Silver tables to create a unified dataset
        - Calculates net transaction amount (amount - refund)
        - Handles null values using ISNULL for consistency
        - Derives business flags:
            * is_refunded
            * is_success
        - Includes fraud risk score for analytical use cases

    Output Columns:
        - Transaction identifiers and dimensions (txn_id, user_id, merchant_id, txn_date)
        - Financial metrics (amount, total_fee, refund_amount, net_amount)
        - Status indicators (status, fraud_flag, is_refunded, is_success)
        - Risk metrics (risk_score)

    Use Cases:
        - Revenue and net sales analysis
        - Refund and failure rate tracking
        - Fraud detection and risk monitoring
        - Building dashboards (Power BI, Tableau)
        - KPI reporting for business stakeholders

Notes:
    - This is a view (not a physical table), ensuring real-time data access
    - Assumes Silver layer data is clean and validated
    - Can be extended with additional dimensions for deeper analysis

Dependencies:
    - Silver layer tables must be populated before execution

Usage:
    SELECT * FROM gold.fact_transactions;

===============================================================================
*/

CREATE VIEW gold.fact_transactions
AS
SELECT
    t.txn_id,
    t.user_id,
    t.merchant_id,
    t.txn_date,
    t.amount,
    ISNULL(f.total_fee, 0) AS total_fee,
    ISNULL(r.refund_amount, 0) AS refund_amount,    
    t.amount - ISNULL(r.refund_amount, 0) AS net_amount,
    t.status,
    t.fraud_flag,
    CASE WHEN r.refund_id IS NOT NULL THEN 1 ELSE 0 END AS is_refunded,
    CASE WHEN t.status = 'success' THEN 1 ELSE 0 END AS is_success,
    ISNULL(fs.risk_score, 0) AS risk_score
FROM silver.erp_transactions t
LEFT JOIN silver.erp_fees f
    ON t.txn_id = f.txn_id
LEFT JOIN silver.erp_refunds r
    ON t.txn_id = r.txn_id
LEFT JOIN silver.erp_fraud_signals fs
    ON t.txn_id = fs.txn_id;
