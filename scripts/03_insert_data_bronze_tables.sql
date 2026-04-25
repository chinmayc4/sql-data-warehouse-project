/*
===============================================================================
File Name   : bronze_layer_data_load.sql
Project     : DataWarehouse - Bronze Layer Data Ingestion
Author      : <Your Name>
Created On  : 2026-04-25
Description : 
    This script performs full-load data ingestion into Bronze layer tables 
    from flat files (CSV). It truncates existing data and reloads fresh 
    data from source system extracts.

    Data Sources:
        - CRM सिस्टम:
            users.csv
            user_activity.csv

        - ERP सिस्टम:
            fees.csv
            fraud_signals.csv
            merchants.csv
            refunds.csv
            transactions.csv

    Target Tables:
        bronze.crm_users
        bronze.crm_user_activity
        bronze.erp_fees
        bronze.erp_fraud_signals
        bronze.erp_merchants
        bronze.erp_refunds
        bronze.erp_transactions

Load Strategy:
    - Full Load (TRUNCATE + BULK INSERT)
    - Header row skipped using FIRSTROW = 2
    - Comma-separated values (CSV)
    - TABLOCK used for performance optimization during bulk load

Notes:
    - File paths are local and should be parameterized for production 
      environments (e.g., Azure Blob, S3, or shared storage).
    - Ensure SQL Server has access to the specified file paths.
    - Data is loaded in raw format; transformations will occur in Silver layer.
    - Duplicate load block detected for 'erp_fraud_signals' (intentional or to review).

Dependencies:
    - Pre-created Bronze tables
    - Access permissions for BULK INSERT
    - Source CSV files available at specified paths

Usage:
    Execute this script after table creation to populate Bronze layer.

Version History:
    v1.0 | 2026-04-25 | Initial data load script
===============================================================================
*/

-- Insert Data to bronze tables

--CRM Tables
--Trunckate table before insert (Full Load)
TRUNCATE TABLE bronze.crm_users;
--Insert data in bronze.crm_users table
BULK INSERT bronze.crm_users
FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\CRM\users.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);

--Trunckate table before insert (Full Load)
TRUNCATE TABLE bronze.crm_user_activity;
--Insert data in bronze.crm_users table
BULK INSERT bronze.crm_user_activity
FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\CRM\user_activity.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);

--ERP Tables
--Trunckate table before insert (Full Load)
TRUNCATE TABLE bronze.erp_fees;
--Insert data in bronze.crm_users table
BULK INSERT bronze.erp_fees
FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\fees.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);

--Trunckate table before insert (Full Load)
TRUNCATE TABLE bronze.erp_fraud_signals;
--Insert data in bronze.crm_users table
BULK INSERT bronze.erp_fraud_signals
FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\fraud_signals.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);

--Trunckate table before insert (Full Load)
TRUNCATE TABLE bronze.erp_fraud_signals;
--Insert data in bronze.crm_users table
BULK INSERT bronze.erp_fraud_signals
FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\fraud_signals.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);

--Trunckate table before insert (Full Load)
TRUNCATE TABLE bronze.erp_merchants;
--Insert data in bronze.crm_users table
BULK INSERT bronze.erp_merchants
FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\merchants.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);

--Trunckate table before insert (Full Load)
TRUNCATE TABLE bronze.erp_refunds;
--Insert data in bronze.crm_users table
BULK INSERT bronze.erp_refunds
FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\refunds.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);


--Trunckate table before insert (Full Load)
TRUNCATE TABLE bronze.erp_transactions;
--Insert data in bronze.crm_users table
BULK INSERT bronze.erp_transactions
FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\transactions.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);

