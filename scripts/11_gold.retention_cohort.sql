/*
===============================================================================
File Name   : gold.retention_cohort.sql
Project     : DataWarehouse - Gold Layer (Cohort Analysis View)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer view 'gold.retention_cohort',
    which enables cohort-based retention analysis by tracking user
    activity over time from their signup month.

    Purpose:
        - Analyze user retention behavior across cohorts
        - Measure how many users remain active over time
        - Support product, growth, and engagement analytics

    Data Sources:
        - gold.dim_users (user signup/cohort information)
        - gold.fact_transactions (user activity)

    Key Transformations:
        - Defines cohort_month based on user signup month
        - Defines activity_month based on transaction month
        - Filters only successful transactions (is_success = 1)
        - Calculates cohort_index as the number of months since signup
        - Counts distinct active users per cohort and activity month

    Output Columns:
        - cohort_month
        - activity_month
        - cohort_index (months since cohort start)
        - active_users

    Use Cases:
        - Retention and cohort analysis
        - User engagement tracking
        - Product performance evaluation
        - Growth and lifecycle analytics
        - Dashboarding (cohort heatmaps in BI tools)

Notes:
        - This is a view (not a physical table), ensuring real-time analysis
        - Cohorts are grouped at monthly granularity
        - Can be extended to calculate retention rates (% of cohort retained)

Dependencies:
        - gold.dim_users must be available
        - gold.fact_transactions must be available

Usage:
        SELECT * FROM gold.retention_cohort;

===============================================================================
*/

CREATE OR ALTER VIEW gold.retention_cohort
AS
WITH user_cohort AS (
    SELECT
        u.user_id,
        DATEFROMPARTS(YEAR(u.signup_date), MONTH(u.signup_date), 1) AS cohort_month
    FROM gold.dim_users u
),
user_activity AS (
    SELECT
        f.user_id,
        DATEFROMPARTS(YEAR(f.txn_date), MONTH(f.txn_date), 1) AS activity_month
    FROM gold.fact_transactions f
    WHERE f.is_success = 1
)
SELECT
    c.cohort_month,
    a.activity_month,
    DATEDIFF(MONTH, c.cohort_month, a.activity_month) AS cohort_index,
    COUNT(DISTINCT a.user_id) AS active_users
FROM user_cohort c
JOIN user_activity a
    ON c.user_id = a.user_id
GROUP BY
    c.cohort_month,
    a.activity_month;
