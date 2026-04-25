/*
===============================================================================
File Name   : gold.customer_segmentation.sql
Project     : DataWarehouse - Gold Layer (Customer Segmentation View)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer view 'gold.customer_segmentation',
    which segments users based on their transaction behavior using
    recency, frequency, and monetary (RFM-like) metrics.

    Purpose:
        - Classify users into meaningful business segments
        - Support targeted marketing and personalization
        - Identify high-value, low-engagement, and churn-risk users

    Data Source:
        - gold.fact_transactions

    Key Transformations:
        - Aggregates user-level metrics:
            * txn_count (frequency)
            * total_spent (monetary)
            * last_txn_date (recency)
        - Calculates recency_days using last transaction date
        - Applies segmentation logic based on:
            * spending thresholds
            * transaction frequency
            * recency of activity

    Segmentation Logic:
        - high_value     : high spend + high frequency users
        - medium_value   : moderate spend users
        - low_engagement : very low transaction count
        - churn_risk     : inactive users (no activity for > 90 days)
        - regular        : remaining users

    Output Columns:
        - user_id
        - txn_count
        - total_spent
        - recency_days
        - customer_segment

    Use Cases:
        - Customer segmentation and profiling
        - Targeted marketing campaigns
        - Retention and churn prevention strategies
        - Business intelligence and dashboarding

Notes:
        - This is a view (not a physical table), ensuring real-time segmentation
        - Only successful transactions are considered
        - Segmentation thresholds can be adjusted based on business needs
        - Can be extended to full RFM scoring model

Dependencies:
        - gold.fact_transactions must be available

Usage:
        SELECT * FROM gold.customer_segmentation;

===============================================================================
*/

CREATE OR ALTER VIEW gold.customer_segmentation
AS
WITH user_metrics AS (
    SELECT
        user_id,
        MAX(txn_date) AS last_txn_date,
        COUNT(*) AS txn_count,
        SUM(net_amount) AS total_spent
    FROM gold.fact_transactions
    WHERE is_success = 1
    GROUP BY user_id
)
SELECT
    user_id,
    txn_count,
    total_spent,
    DATEDIFF(DAY, last_txn_date, GETDATE()) AS recency_days,

    CASE
        WHEN total_spent > 10000 AND txn_count > 20 THEN 'high_value'
        WHEN total_spent > 5000 THEN 'medium_value'
        WHEN txn_count <= 2 THEN 'low_engagement'
        WHEN DATEDIFF(DAY, last_txn_date, GETDATE()) > 90 THEN 'churn_risk'
        ELSE 'regular'
    END AS customer_segment

FROM user_metrics;
