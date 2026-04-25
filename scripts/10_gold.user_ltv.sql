/*
===============================================================================
File Name   : gold.user_ltv.sql
Project     : DataWarehouse - Gold Layer (Aggregate View)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer aggregate view 'gold.user_ltv',
    which calculates lifetime value (LTV) and engagement metrics for
    each user based on transaction history.

    Purpose:
        - Provide user-level revenue and engagement insights
        - Enable customer lifetime value (LTV) analysis
        - Support segmentation and retention strategies

    Data Source:
        - gold.fact_transactions

    Key Transformations:
        - Aggregates transaction data at user level
        - Calculates lifetime revenue metrics:
            * lifetime_gross_revenue
            * lifetime_net_revenue
            * lifetime_fees
        - Derives engagement metrics:
            * total_transactions
            * first_txn_date
            * last_txn_date
            * customer_lifetime_days

    Output Columns:
        - user_id
        - total_transactions
        - lifetime_gross_revenue
        - lifetime_net_revenue
        - lifetime_fees
        - first_txn_date
        - last_txn_date
        - customer_lifetime_days

    Use Cases:
        - Customer lifetime value (LTV) analysis
        - User segmentation (high-value vs low-value users)
        - Retention and churn analysis
        - Marketing and personalization strategies
        - Dashboarding and KPI reporting

Notes:
        - This is a view (not a physical table), ensuring real-time aggregation
        - Assumes fact_transactions contains cleaned and validated data
        - Can be extended with additional KPIs (AOV, frequency, recency)

Dependencies:
        - gold.fact_transactions must be available

Usage:
        SELECT * FROM gold.user_ltv;

===============================================================================
*/

CREATE VIEW gold.user_ltv
AS
SELECT
    f.user_id,
    COUNT(*) AS total_transactions,
    SUM(f.amount) AS lifetime_gross_revenue,
    SUM(f.net_amount) AS lifetime_net_revenue,
    SUM(f.total_fee) AS lifetime_fees,
    MIN(f.txn_date) AS first_txn_date,
    MAX(f.txn_date) AS last_txn_date,
    DATEDIFF(DAY, MIN(f.txn_date), MAX(f.txn_date)) AS customer_lifetime_days
FROM gold.fact_transactions f
GROUP BY f.user_id;
