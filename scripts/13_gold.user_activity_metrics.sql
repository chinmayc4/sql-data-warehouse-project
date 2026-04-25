/*
===============================================================================
File Name   : gold.user_activity_metrics.sql
Project     : DataWarehouse - Gold Layer (User Activity Metrics View)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer view 'gold.user_activity_metrics',
    which provides daily active user (DAU) metrics based on successful
    transactions.

    Purpose:
        - Measure daily user engagement
        - Track active users over time
        - Support KPI reporting and growth analysis

    Data Source:
        - gold.fact_transactions

    Key Transformations:
        - Filters only successful transactions (is_success = 1)
        - Aggregates data at daily granularity
        - Counts distinct users per day to calculate DAU

    Output Columns:
        - activity_date
        - dau (daily active users)

    Use Cases:
        - Daily active user (DAU) tracking
        - User engagement analysis
        - Trend analysis over time
        - Dashboarding (DAU charts in BI tools)

Notes:
        - This is a view (not a physical table), ensuring real-time aggregation
        - DAU is calculated based on transaction activity (not app events)
        - Can be extended to include WAU/MAU metrics

Dependencies:
        - gold.fact_transactions must be available

Usage:
        SELECT * FROM gold.user_activity_metrics;

===============================================================================
*/

CREATE OR ALTER VIEW gold.user_activity_metrics
AS
SELECT
    CAST(txn_date AS DATE) AS activity_date,
    COUNT(DISTINCT user_id) AS dau
FROM gold.fact_transactions
WHERE is_success = 1
GROUP BY CAST(txn_date AS DATE);
