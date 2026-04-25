/*
===============================================================================
File Name   : silver_layer_tables.sql
Project     : DataWarehouse - Silver Layer Setup
Author      : Chinmay Pisu

Description : 
    This script creates all required tables for the Silver layer in the 
    Data Warehouse. The Silver layer stores cleaned, standardized, and 
    lightly transformed data derived from the Bronze layer.

    Enhancements over Bronze Layer:
        - Addition of dwh_create_time for audit and lineage tracking
        - Structured schema for downstream transformations
        - Prepared for data type standardization and cleansing

    Tables Covered:
        - CRM Tables:
            silver.crm_users
            silver.crm_user_activity

        - ERP Tables:
            silver.erp_fees
            silver.erp_fraud_signals
            silver.erp_merchants
            silver.erp_refunds
            silver.erp_transactions

Notes:
    - Existing tables are dropped before creation to maintain idempotency.
    - dwh_create_time captures the record load timestamp.
    - Some columns are intentionally kept as NVARCHAR for initial standardization;
      further casting and enrichment can be applied in transformation pipelines.
    - This layer acts as the foundation for the Gold (business) layer.

Dependencies:
    - Database: DataWarehouse
    - Schemas: silver (must exist prior to execution)

Usage:
    Execute this script after Bronze layer setup to initialize Silver tables.
===============================================================================
*/
--Create all required tables for silver layer

IF OBJECT_ID('silver.crm_users','U') IS NOT NULL
	DROP TABLE silver.crm_users;
-- Create crm_user table
CREATE TABLE silver.crm_users (
	user_id INT,
	name NVARCHAR(100),
	email NVARCHAR(100),
	signup_date DATE,
	country NVARCHAR(50),
	device_type NVARCHAR(50),
	acquisition_channel NVARCHAR(50),
	dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_user_activity','U') IS NOT NULL
	DROP TABLE silver.crm_user_activity;
-- Create crm_user_activity
CREATE TABLE silver.crm_user_activity (
	event_id INT,
	user_id INT,
	event_type NVARCHAR(50),
	event_time DATETIME2,
	device NVARCHAR(50),
	dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_fees','U') IS NOT NULL
	DROP TABLE silver.erp_fees;
--Create erp_fees
CREATE TABLE silver.erp_fees (
	txn_id INT,
	processing_fee DECIMAL(18,2),
	service_fee DECIMAL(18,2),
	fx_fee DECIMAL(18,2),
	total_fee DECIMAL(18,2),
	dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_fraud_signals','U') IS NOT NULL
	DROP TABLE silver.erp_fraud_signals;
--Create erp_fraud_signals
CREATE TABLE silver.erp_fraud_signals (
	txn_id INT,
	ip_address NVARCHAR(50),
	location NVARCHAR(50),
	device_id NVARCHAR(50),
	risk_score DECIMAL(5,3),
	dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_merchants','U') IS NOT NULL
	DROP TABLE silver.erp_merchants;
--Create erp_merchants
CREATE TABLE silver.erp_merchants (
	merchant_id INT,	
	merchant_name NVARCHAR(50),	
	category NVARCHAR(50),	
	country	NVARCHAR(50),
	onboard_date DATE,	
	risk_score DECIMAL(5,3),
	dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_refunds','U') IS NOT NULL
	DROP TABLE silver.erp_refunds;
--Create erp_refunds
CREATE TABLE silver.erp_refunds (
	refund_id INT,	
	txn_id	INT,
	refund_amount DECIMAL(18,2),	
	refund_date	DATETIME2,
	reason NVARCHAR(50),
	dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_transactions','U') IS NOT NULL
	DROP TABLE silver.erp_transactions;
--Create erp_transactions
CREATE TABLE silver.erp_transactions (
	txn_id INT,
	user_id	INT,
	txn_date DATETIME2, 	
	amount DECIMAL(18,2),	
	currency NVARCHAR(50),	
	payment_method NVARCHAR(50),
	merchant_id	INT,
	status	NVARCHAR(50),
	fraud_flag BIT,
	dwh_create_time DATETIME2 DEFAULT GETDATE()
);
