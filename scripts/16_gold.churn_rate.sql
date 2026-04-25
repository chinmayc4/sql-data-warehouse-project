/*
===============================================================================
File Name   : gold.churn_rate.sql
Project     : DataWarehouse - Gold Layer (User Retention Metrics View)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer view 'gold.churn_rate',
    which calculates monthly churn metrics by comparing active
    users between consecutive months.

    Purpose:
        - Measure user churn over time
        - Identify retention and drop-off trends
        - Support growth and lifecycle analysis

    Data Source:
        - gold.fact_transactions

    Key Transformations:
        - Identifies monthly active users based on successful transactions
        - Creates a shifted dataset (prev_users) to align previous month users
        - Compares previous vs current month user base
        - Calculates:
            * previous_users
            * current_users
            * churned_users
            * churn_rate
        - Uses NULLIF to avoid division by zero

    Output Columns:
        - month
        - previous_users
        - current_users
        - churned_users
        - churn_rate

    Use Cases:
        - Churn and retention analysis
        - User lifecycle monitoring
        - Product and growth performance tracking
        - Dashboarding (monthly churn trends)

Notes:
        - This is a view (not a physical table), ensuring real-time calculation
        - Churn is defined as users active in previous month but not in current month
        - Only successful transactions are considered for activity
        - Can be extended with retention rate and cohort-based churn

Dependencies:
        - gold.fact_transactions must be available

Usage:
        SELECT * FROM gold.churn_rate;

===============================================================================
*/

CREATE VIEW gold.churn_rate
AS
WITH monthly_users AS (
    SELECT
        DATEFROMPARTS(YEAR(txn_date), MONTH(txn_date), 1) AS month,
        user_id
    FROM gold.fact_transactions
    WHERE is_success = 1
    GROUP BY DATEFROMPARTS(YEAR(txn_date), MONTH(txn_date), 1), user_id
),
prev_users AS (
    SELECT
        DATEADD(MONTH, 1, month) AS month,
        user_id
    FROM monthly_users
)
SELECT
    m.month,

    COUNT(DISTINCT p.user_id) AS previous_users,

    COUNT(DISTINCT m.user_id) AS current_users,

    COUNT(DISTINCT p.user_id) 
        - COUNT(DISTINCT m.user_id) AS churned_users,

    (COUNT(DISTINCT p.user_id) 
        - COUNT(DISTINCT m.user_id)) * 1.0
        / NULLIF(COUNT(DISTINCT p.user_id), 0) AS churn_rate

FROM prev_users p
LEFT JOIN monthly_users m
    ON p.user_id = m.user_id
    AND p.month = m.month

GROUP BY m.month;
