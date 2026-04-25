/*
===============================================================================
File Name   : gold.fraud_anomalies.sql
Project     : DataWarehouse - Gold Layer (Fraud Detection View)
Author      : Chinmay Pisu

Description : 
    This script creates the Gold layer view 'gold.fraud_anomalies',
    which identifies potentially fraudulent transactions using
    rule-based anomaly detection.

    Purpose:
        - Flag high-risk and suspicious transactions
        - Support fraud monitoring and investigation
        - Enable early detection of anomalous behavior

    Data Source:
        - gold.fact_transactions

    Key Transformations:
        - Evaluates transactions based on:
            * transaction amount thresholds
            * fraud risk score thresholds
        - Derives anomaly indicators:
            * high_amount_flag (amount > 900)
            * high_risk_flag (risk_score > 0.8)
            * fraud_suspected (both conditions met)

    Output Columns:
        - txn_id
        - user_id
        - amount
        - risk_score
        - txn_date
        - high_amount_flag
        - high_risk_flag
        - fraud_suspected

    Use Cases:
        - Fraud detection and alerting systems
        - Risk-based transaction monitoring
        - Investigation of suspicious activities
        - Dashboarding for fraud analytics teams

Notes:
        - This is a view (not a physical table), ensuring real-time detection
        - Thresholds (amount, risk_score) are rule-based and configurable
        - Can be extended with ML-based fraud detection models

Dependencies:
        - gold.fact_transactions must be available

Usage:
        SELECT * FROM gold.fraud_anomalies;

===============================================================================
*/

CREATE OR ALTER VIEW gold.fraud_anomalies
AS
SELECT
    txn_id,
    user_id,
    amount,
    risk_score,
    txn_date,
    CASE WHEN amount > 900 THEN 1 ELSE 0 END AS high_amount_flag,
    CASE WHEN risk_score > 0.8 THEN 1 ELSE 0 END AS high_risk_flag,
    CASE 
        WHEN amount > 900 AND risk_score > 0.8 THEN 1 
        ELSE 0 
    END AS fraud_suspected
FROM gold.fact_transactions;
