/*
===============================================================================
File Name   : bronze_layer_tables.sql
Project     : DataWarehouse - Bronze Layer Setup
Author      : <Your Name>
Created On  : 2026-04-25
Description : 
    This script creates all required tables for the Bronze layer in the 
    Data Warehouse. The Bronze layer stores raw, ingested data from 
    source systems (CRM and ERP) with minimal transformation.

    Tables Covered:
        - CRM Tables:
            bronze.crm_users
            bronze.crm_user_activity

        - ERP Tables:
            bronze.erp_transactions
            bronze.erp_fees
            bronze.erp_fraud_signals
            bronze.erp_merchants
            bronze.erp_refunds

Notes:
    - Existing tables are dropped before creation to ensure idempotency.
    - Data types are kept as-is to reflect raw ingestion format.
    - Further transformations and type casting will be handled in Silver layer.

Dependencies:
    - Database: DataWarehouse
    - Schemas: bronze (must exist prior to execution)

Usage:
    Execute this script in SQL Server to initialize Bronze layer tables.

Version History:
    v1.0 | 2026-04-25 | Initial creation
===============================================================================
*/

IF OBJECT_ID('bronze.crm_users','U') IS NOT NULL
	DROP TABLE bronze.crm_users;
-- Create crm_user table
CREATE TABLE bronze.crm_users (
	user_id INT,
	name NVARCHAR(50),
	email NVARCHAR(50),
	signup_date DATE,
	country NVARCHAR(50),
	device_type NVARCHAR(50),
	acquisition_channel NVARCHAR(50)
);

IF OBJECT_ID('bronze.crm_user_activity','U') IS NOT NULL
	DROP TABLE bronze.crm_user_activity;
-- Create crm_user_activity
CREATE TABLE bronze.crm_user_activity (
	event_id INT,
	user_id NVARCHAR(50),
	event_type NVARCHAR(50),
	event_time DATETIME,
	device NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_fees','U') IS NOT NULL
	DROP TABLE bronze.erp_fees;
--Create erp_fees
CREATE TABLE bronze.erp_fees (
	txn_id INT,
	processing_fee NVARCHAR(50),
	service_fee NVARCHAR(50),
	fx_fee NVARCHAR(50),
	total_fee NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_fraud_signals','U') IS NOT NULL
	DROP TABLE bronze.erp_fraud_signals;
--Create erp_fraud_signals
CREATE TABLE bronze.erp_fraud_signals (
	txn_id INT,
	ip_address NVARCHAR(50),
	location NVARCHAR(50),
	device_id NVARCHAR(50),
	risk_score NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_merchants','U') IS NOT NULL
	DROP TABLE bronze.erp_merchants;
--Create erp_merchants
CREATE TABLE bronze.erp_merchants (
	merchant_id INT,	
	merchant_name NVARCHAR(50),	
	category NVARCHAR(50),	
	country	NVARCHAR(50),
	onboard_date DATE,	
	risk_score NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_refunds','U') IS NOT NULL
	DROP TABLE bronze.erp_refunds;
--Create erp_refunds
CREATE TABLE bronze.erp_refunds (
	refund_id INT,	
	txn_id	INT,
	refund_amount NVARCHAR(50),	
	refund_date	DATE,
	reason NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_transactions','U') IS NOT NULL
	DROP TABLE bronze.erp_transactions;
--Create erp_transactions
CREATE TABLE bronze.erp_transactions (
	txn_id INT,
	user_id	INT,
	txn_date DATE, 	
	amount NVARCHAR(50),	
	currency NVARCHAR(50),	
	payment_method NVARCHAR(50),
	merchant_id	INT,
	status	NVARCHAR(50),
	fraud_flag INT
);
