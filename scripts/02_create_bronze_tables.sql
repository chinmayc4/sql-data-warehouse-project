/*
===============================================================================
File Name   : bronze_layer_setup.sql
Project     : DataWarehouse - Bronze Layer Setup
Author      : Chinmay Pisu

Description : 
    This script initializes the Bronze layer by creating all required
    schemas and tables for raw data ingestion from CRM and ERP systems.

    Purpose:
        - Set up the foundational layer for raw data storage
        - Ensure schema availability before table creation
        - Maintain idempotency by dropping and recreating tables

    Data Coverage:
        - CRM Tables:
            * crm_users
            * crm_user_activity

        - ERP Tables:
            * erp_transactions
            * erp_fees
            * erp_fraud_signals
            * erp_merchants
            * erp_refunds

    Key Features:
        - Uses transaction control (BEGIN TRANSACTION / COMMIT / ROLLBACK)
        - Ensures schema existence dynamically
        - Drops existing tables before recreation (idempotent execution)
        - Stores data in raw format (minimal transformation)
        - Prepares data for downstream processing in Silver layer

    Design Notes:
        - Most numeric fields are stored as NVARCHAR for raw ingestion
        - Data type casting and validation are handled in Silver layer
        - User activity user_id is stored as NVARCHAR to match source format

    Error Handling:
        - TRY...CATCH block implemented
        - Automatic rollback on failure
        - Error message printed for debugging

    Dependencies:
        - SQL Server environment
        - Appropriate permissions for schema and table creation

    Usage:
        Execute this script to initialize Bronze layer before data loading.

===============================================================================
*/

BEGIN TRY
BEGIN TRANSACTION;

----------------------------------------------------------------------------
-- Ensure Bronze Schema Exists
----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.schemas WHERE name = 'bronze'
)
BEGIN
    EXEC('CREATE SCHEMA bronze');
END;

----------------------------------------------------------------------------
-- CRM TABLES
----------------------------------------------------------------------------

-- Drop & Create: crm_users
DROP TABLE IF EXISTS bronze.crm_users;
CREATE TABLE bronze.crm_users (
    user_id INT,
    name NVARCHAR(50),
    email NVARCHAR(50),
    signup_date DATE,
    country NVARCHAR(50),
    device_type NVARCHAR(50),
    acquisition_channel NVARCHAR(50)
);

-- Drop & Create: crm_user_activity
DROP TABLE IF EXISTS bronze.crm_user_activity;
CREATE TABLE bronze.crm_user_activity (
    event_id INT,
    user_id NVARCHAR(50), -- raw ingestion format
    event_type NVARCHAR(50),
    event_time DATETIME,
    device NVARCHAR(50)
);

----------------------------------------------------------------------------
-- ERP TABLES
----------------------------------------------------------------------------

-- Drop & Create: erp_transactions
DROP TABLE IF EXISTS bronze.erp_transactions;
CREATE TABLE bronze.erp_transactions (
    txn_id INT,
    user_id INT,
    txn_date DATE,
    amount NVARCHAR(50), -- raw format (to be cast in Silver)
    currency NVARCHAR(50),
    payment_method NVARCHAR(50),
    merchant_id INT,
    status NVARCHAR(50),
    fraud_flag INT
);

-- Drop & Create: erp_fees
DROP TABLE IF EXISTS bronze.erp_fees;
CREATE TABLE bronze.erp_fees (
    txn_id INT,
    processing_fee NVARCHAR(50),
    service_fee NVARCHAR(50),
    fx_fee NVARCHAR(50),
    total_fee NVARCHAR(50)
);

-- Drop & Create: erp_fraud_signals
DROP TABLE IF EXISTS bronze.erp_fraud_signals;
CREATE TABLE bronze.erp_fraud_signals (
    txn_id INT,
    ip_address NVARCHAR(50),
    location NVARCHAR(50),
    device_id NVARCHAR(50),
    risk_score NVARCHAR(50) -- raw format
);

-- Drop & Create: erp_merchants
DROP TABLE IF EXISTS bronze.erp_merchants;
CREATE TABLE bronze.erp_merchants (
    merchant_id INT,
    merchant_name NVARCHAR(50),
    category NVARCHAR(50),
    country NVARCHAR(50),
    onboard_date DATE,
    risk_score NVARCHAR(50) -- raw format
);

-- Drop & Create: erp_refunds
DROP TABLE IF EXISTS bronze.erp_refunds;
CREATE TABLE bronze.erp_refunds (
    refund_id INT,
    txn_id INT,
    refund_amount NVARCHAR(50), -- raw format
    refund_date DATE,
    reason NVARCHAR(50)
);

----------------------------------------------------------------------------
-- Commit Transaction
----------------------------------------------------------------------------
COMMIT TRANSACTION;

END TRY
BEGIN CATCH

----------------------------------------------------------------------------
-- Rollback on Failure
----------------------------------------------------------------------------
ROLLBACK TRANSACTION;

PRINT 'Error occurred in Bronze Layer Setup: ' + ERROR_MESSAGE();

END CATCH;
