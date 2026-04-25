/*
===============================================================================
File Name   : gold.funnel_analysis.sql
Project     : DataWarehouse - Gold Layer (Funnel Analysis View)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer view 'gold.funnel_analysis',
    which provides a daily breakdown of user activity events to
    analyze the user journey and conversion funnel.

    Purpose:
        - Track user progression through key application events
        - Analyze conversion rates across funnel stages
        - Support product and growth analytics

    Data Source:
        - silver.crm_user_activity

    Key Transformations:
        - Aggregates user activity at daily granularity
        - Counts occurrences of key funnel events:
            * login
            * view
            * payment_attempt
            * logout
        - Uses conditional aggregation for event-level metrics

    Output Columns:
        - event_date
        - logins
        - views
        - payment_attempts
        - logouts

    Use Cases:
        - Funnel conversion analysis
        - Drop-off point identification
        - User behavior tracking
        - Product performance monitoring
        - Dashboarding (conversion funnel visualization)

Notes:
        - This is a view (not a physical table), ensuring real-time aggregation
        - Assumes event_type values are standardized in Silver layer
        - Can be extended with conversion rate calculations between stages

Dependencies:
        - silver.crm_user_activity must be available

Usage:
        SELECT * FROM gold.funnel_analysis;

===============================================================================
*/

CREATE OR ALTER VIEW gold.funnel_analysis
AS
SELECT
    CAST(event_time AS DATE) AS event_date,
    COUNT(CASE WHEN event_type = 'login' THEN 1 END) AS logins,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS views,
    COUNT(CASE WHEN event_type = 'payment_attempt' THEN 1 END) AS payment_attempts,
    COUNT(CASE WHEN event_type = 'logout' THEN 1 END) AS logouts
FROM silver.crm_user_activity
GROUP BY CAST(event_time AS DATE);
