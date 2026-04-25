/*
===============================================================================
File Name   : bronze_layer_data_load.sql
Project     : DataWarehouse - Bronze Layer Data Ingestion
Author      : Chinmay Pisu
Description : 
    This script performs full-load data ingestion into Bronze layer tables 
    from flat files (CSV). It truncates existing data and reloads fresh 
    data from source system extracts.

    Data Sources:
        - CRM:
            users.csv
            user_activity.csv

        - ERP:
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

===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME; 

    SET @batch_start_time = GETDATE();

    BEGIN TRY
        PRINT('============================================================');
        PRINT('                   Loading Bronze Layer                     ');
        PRINT('============================================================');

        ------------------------------------------------------------
        -- CRM Tables
        ------------------------------------------------------------
        PRINT('--------------------------------------------');
        PRINT('            Loading CRM Tables              ');
        PRINT('--------------------------------------------');

        ------------------------------------------------------------
        -- bronze.crm_users
        ------------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT('---------------------');
        PRINT('>>> Truncating Table: bronze.crm_users');
        TRUNCATE TABLE bronze.crm_users;

        PRINT('>>> Inserting Table: bronze.crm_users');
        BULK INSERT bronze.crm_users
        FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\CRM\users.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT('---------------------');

        ------------------------------------------------------------
        -- bronze.crm_user_activity
        ------------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT('---------------------');
        PRINT('>>> Truncating Table: bronze.crm_user_activity');
        TRUNCATE TABLE bronze.crm_user_activity;

        PRINT('>>> Inserting Data Into: bronze.crm_user_activity');
        BULK INSERT bronze.crm_user_activity
        FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\CRM\user_activity.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT('---------------------');

        ------------------------------------------------------------
        -- ERP Tables
        ------------------------------------------------------------
        PRINT('--------------------------------------------');
        PRINT('            Loading ERP Tables              ');
        PRINT('--------------------------------------------');

        ------------------------------------------------------------
        -- bronze.erp_fees
        ------------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT('---------------------');
        PRINT('>>> Truncating Table: erp_fees');
        TRUNCATE TABLE bronze.erp_fees;

        PRINT('>>> Inserting Data Into: erp_fees');
        BULK INSERT bronze.erp_fees
        FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\fees.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT('---------------------');

        ------------------------------------------------------------
        -- bronze.erp_fraud_signals
        ------------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT('---------------------');
        PRINT('>>> Truncating Table: erp_fraud_signals');
        TRUNCATE TABLE bronze.erp_fraud_signals;

        PRINT('>>> Inserting Data Into: erp_fraud_signals');
        BULK INSERT bronze.erp_fraud_signals
        FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\fraud_signals.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT('---------------------');

        ------------------------------------------------------------
        -- bronze.erp_merchants
        ------------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT('---------------------');
        PRINT('>>> Truncating Table: bronze.erp_merchants');
        TRUNCATE TABLE bronze.erp_merchants;

        PRINT('>>> Inserting Data Into: bronze.erp_merchants');
        BULK INSERT bronze.erp_merchants
        FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\merchants.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            FIELDQUOTE = '"',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT('---------------------');

        ------------------------------------------------------------
        -- bronze.erp_refunds
        ------------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT('---------------------');
        PRINT('>>> Truncating Table: bronze.erp_refunds');
        TRUNCATE TABLE bronze.erp_refunds;

        PRINT('>>> Inserting Data Into: bronze.erp_refunds');
        BULK INSERT bronze.erp_refunds
        FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\refunds.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT('---------------------');

        ------------------------------------------------------------
        -- bronze.erp_transactions
        ------------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT('---------------------');
        PRINT('>>> Truncating Table: bronze.erp_transactions');
        TRUNCATE TABLE bronze.erp_transactions;

        PRINT('>>> Inserting Data Into: bronze.erp_transactions');
        BULK INSERT bronze.erp_transactions
        FROM 'C:\Users\squar\Desktop\D\SQL\Dataset\ERP\transactions.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT('---------------------');

    END TRY
    BEGIN CATCH
        PRINT('------------------------------------------------');
        PRINT('ERROR OCCURED DURING LOADING BRONZE LAYER');
        PRINT 'ERROR MESSAGE ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR STATE ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT('------------------------------------------------');
    END CATCH;

    SET @batch_end_time = GETDATE();

    PRINT('================================');
    PRINT('Loading Bronze Layer Completed');
    PRINT '>> Loading Duration ETL: ' 
        + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
        + ' seconds';
    PRINT('================================');

END;
