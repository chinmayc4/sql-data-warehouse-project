/*
===============================================================================
File Name   : gold.dim_users.sql
Project     : DataWarehouse - Gold Layer (Dimension Table)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer dimension view 'gold.dim_users',
    which provides a cleaned and enriched user dataset for analytics
    and reporting purposes.

    Purpose:
        - Serve as a dimension table for user-level analysis
        - Enable segmentation based on user lifecycle
        - Support joins with fact tables (e.g., fact_transactions)

    Data Source:
        - silver.crm_users

    Key Transformations:
        - Retains standardized user attributes from Silver layer
        - Calculates user_age_days using signup_date
        - Derives user_segment based on user tenure:
            * new (<= 30 days)
            * active (31–180 days)
            * old (> 180 days)

    Output Columns:
        - User attributes (user_id, country, device_type, acquisition_channel)
        - Temporal metrics (signup_date, user_age_days)
        - Segmentation (user_segment)

    Use Cases:
        - User cohort and lifecycle analysis
        - Customer segmentation
        - Behavioral analytics when joined with fact tables
        - Dashboarding and KPI reporting

Notes:
        - This is a view (not a physical table), ensuring real-time data access
        - Assumes Silver layer data is cleaned and validated
        - Segmentation logic can be adjusted as per business requirements

Dependencies:
        - silver.crm_users must be populated

Usage:
        SELECT * FROM gold.dim_users;

===============================================================================
*/

CREATE OR ALTER VIEW gold.dim_users
AS
SELECT
    user_id,
    country,
    device_type,
    acquisition_channel,
    signup_date,
    DATEDIFF(DAY, signup_date, GETDATE()) AS user_age_days,
    CASE 
        WHEN DATEDIFF(DAY, signup_date, GETDATE()) <= 30 THEN 'new'
        WHEN DATEDIFF(DAY, signup_date, GETDATE()) <= 180 THEN 'active'
        ELSE 'old'
    END AS user_segment
FROM silver.crm_users;
