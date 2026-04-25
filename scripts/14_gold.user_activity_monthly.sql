/*
===============================================================================
File Name   : gold.user_activity_monthly.sql
Project     : DataWarehouse - Gold Layer (User Activity Metrics View)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer view 'gold.user_activity_monthly',
    which provides monthly active user (MAU) metrics based on successful
    transactions.

    Purpose:
        - Measure monthly user engagement
        - Track active users at monthly granularity
        - Support growth and retention analysis

    Data Source:
        - gold.fact_transactions

    Key Transformations:
        - Filters only successful transactions (is_success = 1)
        - Aggregates data at monthly granularity
        - Uses DATEFROMPARTS to normalize dates to month level
        - Counts distinct users per month to calculate MAU

    Output Columns:
        - activity_month
        - mau (monthly active users)

    Use Cases:
        - Monthly active user (MAU) tracking
        - User engagement trend analysis
        - Cohort and retention insights
        - Dashboarding (MAU trends in BI tools)

Notes:
        - This is a view (not a physical table), ensuring real-time aggregation
        - MAU is calculated based on transaction activity (not app events)
        - Can be extended to derive WAU, DAU/MAU ratios, etc.

Dependencies:
        - gold.fact_transactions must be available

Usage:
        SELECT * FROM gold.user_activity_monthly;

===============================================================================
*/

CREATE VIEW gold.user_activity_monthly
AS
SELECT
    DATEFROMPARTS(YEAR(txn_date), MONTH(txn_date), 1) AS activity_month,
    COUNT(DISTINCT user_id) AS mau
FROM gold.fact_transactions
WHERE is_success = 1
GROUP BY DATEFROMPARTS(YEAR(txn_date), MONTH(txn_date), 1);
