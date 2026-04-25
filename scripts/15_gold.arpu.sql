/*
===============================================================================
File Name   : gold.arpu.sql
Project     : DataWarehouse - Gold Layer (Revenue Metrics View)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer view 'gold.arpu',
    which calculates Average Revenue Per User (ARPU) at a daily level
    based on transaction data.

    Purpose:
        - Measure monetization efficiency per user
        - Track revenue trends relative to active user base
        - Support business and product performance analysis

    Data Source:
        - gold.fact_transactions

    Key Transformations:
        - Aggregates data at daily granularity
        - Calculates ARPU as:
              total net revenue / distinct users
        - Uses NULLIF to avoid division by zero errors

    Output Columns:
        - txn_date
        - arpu (average revenue per user)

    Use Cases:
        - Monetization analysis
        - KPI tracking for revenue per user
        - Comparing performance across time periods
        - Dashboarding (ARPU trends in BI tools)

Notes:
        - This is a view (not a physical table), ensuring real-time calculation
        - ARPU is calculated using net_amount (after refunds)
        - Can be extended to weekly/monthly ARPU

Dependencies:
        - gold.fact_transactions must be available

Usage:
        SELECT * FROM gold.arpu;

===============================================================================
*/

CREATE OR ALTER VIEW gold.arpu
AS
SELECT
    CAST(txn_date AS DATE) AS txn_date,
    SUM(net_amount) * 1.0 / NULLIF(COUNT(DISTINCT user_id), 0) AS arpu
FROM gold.fact_transactions
GROUP BY CAST(txn_date AS DATE);
