/*
===============================================================================
File Name   : gold.dim_merchants.sql
Project     : DataWarehouse - Gold Layer (Dimension Table)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer dimension view 'gold.dim_merchants',
    which provides a cleaned and enriched merchant dataset for analytics
    and reporting purposes.

    Purpose:
        - Serve as a dimension table for merchant-related analysis
        - Enable segmentation and profiling of merchants
        - Support joins with fact tables (e.g., fact_transactions)

    Data Source:
        - silver.erp_merchants

    Key Transformations:
        - Retains standardized merchant attributes from Silver layer
        - Calculates merchant_age_days using onboard_date
        - Derives risk_segment based on risk_score thresholds:
            * high_risk (>= 0.8)
            * medium_risk (>= 0.5 and < 0.8)
            * low_risk (< 0.5)

    Output Columns:
        - Merchant attributes (merchant_id, merchant_name, category, country)
        - Risk metrics (risk_score, risk_segment)
        - Temporal metric (merchant_age_days)

    Use Cases:
        - Merchant performance analysis
        - Risk-based segmentation
        - Joining with fact tables for reporting and dashboards
        - Business intelligence and KPI tracking

Notes:
        - This is a view (not a physical table), ensuring real-time data access
        - Assumes Silver layer data is cleaned and validated
        - Risk segmentation logic can be adjusted based on business rules

Dependencies:
        - silver.erp_merchants must be populated

Usage:
        SELECT * FROM gold.dim_merchants;

===============================================================================
*/

CREATE VIEW gold.dim_merchants
AS
SELECT
    merchant_id,
    merchant_name,
    category,
    country,
    risk_score,
    onboard_date,
    DATEDIFF(DAY, onboard_date, GETDATE()) AS merchant_age_days,
    CASE 
        WHEN risk_score >= 0.8 THEN 'high_risk'
        WHEN risk_score >= 0.5 THEN 'medium_risk'
        ELSE 'low_risk'
    END AS risk_segment
FROM silver.erp_merchants;
