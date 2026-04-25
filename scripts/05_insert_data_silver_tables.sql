/*
===============================================================================
File Name   : silver_load_procedure.sql
Project     : DataWarehouse - Silver Layer Transformation
Author      : Chinmay Pisu

Description : 
    This script creates and executes the stored procedure 
    'silver.load_silver' responsible for loading and transforming data 
    from the Bronze layer into the Silver layer.

    Transformation Logic:
        - Data cleansing using TRIM, LOWER, UPPER
        - Deduplication using ROW_NUMBER() window functions
        - Data type standardization using TRY_CONVERT
        - Filtering invalid and inconsistent records
        - Enforcing business rules (e.g., valid transaction amounts, risk score range)

    Processing Strategy:
        - Full Load approach (TRUNCATE + INSERT)
        - Sequential loading of CRM and ERP entities
        - Dependency-aware joins (users → transactions → downstream tables)
        - Audit tracking using dwh_create_time

    Tables Processed:
        - CRM:
            silver.crm_users
            silver.crm_user_activity

        - ERP:
            silver.erp_merchants
            silver.erp_transactions
            silver.erp_fees
            silver.erp_fraud_signals
            silver.erp_refunds

    Key Features:
        - Data validation (null checks, ranges, referential integrity)
        - Standardization of categorical fields
        - Handling of duplicate and late-arriving data
        - Execution time logging for each table load
        - Error handling using TRY...CATCH block

Notes:
    - This procedure depends on fully populated Bronze tables
    - Designed for batch ETL execution
    - Can be integrated with orchestration tools (ADF, Airflow, etc.)
    - Some columns are cast dynamically and may require optimization in production

Dependencies:
    - Bronze Layer Tables
    - Silver Layer Tables
    - SQL Server permissions for procedure execution

Usage:
    EXEC silver.load_silver;

===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME; 
	SET @batch_start_time = GETDATE();
		BEGIN TRY
			PRINT('============================================================')
			PRINT('                   Loading Silver Layer                     ')
			PRINT('============================================================')
				-- Insert Data to bronze tables
				--CRM Tables
				PRINT('--------------------------------------------')
				PRINT('            Loading CRM Tables              ')
				PRINT('--------------------------------------------')
				--Truncate table before insert (Full Load)
				----------------------------------------------------------------------------------------------------------------------
				SET @start_time = GETDATE();
				PRINT('---------------------')
				PRINT('>>> Truncating Table: silver.crm_users')
				TRUNCATE TABLE silver.crm_users;
				--Insert data in bronze.crm_users table
				PRINT('>>> Inserting Table: silver.crm_users')
				INSERT INTO silver.crm_users(
					user_id,
					name,
					email,
					signup_date,
					country,
					device_type,
					acquisition_channel,
					dwh_create_time
				)
				SELECT
					user_id,
					TRIM(name) AS name,
					LOWER(TRIM(email)) AS email,
					signup_date,
					UPPER(TRIM(country)) AS country,
					TRIM(device_type),
					TRIM(acquisition_channel),
					GETDATE()
				FROM (
					SELECT *,
						ROW_NUMBER() OVER(PARTITION BY LOWER(TRIM(email)) ORDER BY signup_date DESC) AS rn
					FROM bronze.crm_users
					WHERE user_id IS NOT NULL
						AND email IS NOT NULL
				) t
				WHERE rn = 1;
				SET @end_time = GETDATE();
				PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
				PRINT('---------------------')
				----------------------------------------------------------------------------------------------------------------------
				--Trunckate table before insert (Full Load)
				SET @start_time = GETDATE();
				PRINT('---------------------')
				PRINT('>>> Truncating Table: silver.crm_user_activity')
				TRUNCATE TABLE silver.crm_user_activity;
				--Insert data in bronze.crm_users table
				PRINT('>>> Inserting Data Into: silver.crm_user_activity')
				INSERT INTO silver.crm_user_activity (
					event_id,
					user_id,
					event_type,
					event_time,
					device,
					dwh_create_time
				)
				SELECT
					event_id,
					user_id,
					LOWER(TRIM(event_type)) AS event_type,
					event_time,
					TRIM(device),
					GETDATE()
				FROM (
					SELECT a.*,
						ROW_NUMBER() OVER (
							PARTITION BY event_id
							ORDER BY event_time DESC
						) AS rn
					FROM bronze.crm_user_activity a
					INNER JOIN silver.crm_users u
						ON a.user_id = u.user_id
					WHERE a.user_id IS NOT NULL
						AND a.event_time IS NOT NULL
						AND a.event_time <= GETDATE()
						AND LOWER(TRIM(a.event_type)) IN ('login','view','payment_attempt','logout')
				) t
				WHERE rn = 1;
				SET @end_time = GETDATE();
				PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
				PRINT('---------------------')
				----------------------------------------------------------------------------------------------------------------------
			--ERP Tables
			PRINT('--------------------------------------------')
			PRINT('            Loading ERP Tables              ')
			PRINT('--------------------------------------------')
				--Trunckate table before insert (Full Load)
				SET @start_time = GETDATE();
				PRINT('---------------------')
				PRINT('>>> Truncating Table: silver.erp_merchants')
				TRUNCATE TABLE silver.erp_merchants;
				--Insert data in bronze.crm_users table
				PRINT('>>> Inserting Data Into: silver.erp_merchants')		
				INSERT INTO silver.erp_merchants (
					merchant_id,
					merchant_name,
					category,
					country,
					onboard_date,
					risk_score,
					dwh_create_time
				)
				SELECT
					merchant_id,
					merchant_name,
					category,
					country,
					onboard_date,
					risk_score,
					GETDATE()
				FROM (
					SELECT
						m.merchant_id,
						TRIM(m.merchant_name) AS merchant_name,
						TRIM(m.category) AS category,
						UPPER(TRIM(m.country)) AS country,        
						COALESCE(
							TRY_CONVERT(DATE, m.onboard_date, 120), 
							TRY_CONVERT(DATE, m.onboard_date, 101), 
							TRY_CONVERT(DATE, m.onboard_date, 103), 
							TRY_CONVERT(DATE, m.onboard_date)
						) AS onboard_date,        
						TRY_CONVERT(DECIMAL(5,3), m.risk_score) AS risk_score,
						ROW_NUMBER() OVER ( PARTITION BY m.merchant_id ORDER BY 
								COALESCE(
									TRY_CONVERT(DATE, m.onboard_date, 120),
									TRY_CONVERT(DATE, m.onboard_date, 101),
									TRY_CONVERT(DATE, m.onboard_date, 103),
									TRY_CONVERT(DATE, m.onboard_date)
								) DESC
						) AS rn
					FROM bronze.erp_merchants m
					WHERE m.merchant_id IS NOT NULL
				) t
				WHERE rn = 1
				AND onboard_date IS NOT NULL
				AND risk_score BETWEEN 0 AND 1;
				SET @end_time = GETDATE();
				PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
				PRINT('---------------------')
				----------------------------------------------------------------------------------------------------------------------
				--Trunckate table before insert (Full Load)
				SET @start_time = GETDATE();
				PRINT('---------------------')
				PRINT('>>> Truncating Table: silver.erp_transactions')
				TRUNCATE TABLE silver.erp_transactions;
				--Insert data in bronze.crm_users table
				PRINT('>>> Inserting Data Into: silver.erp_transactions')		
				INSERT INTO silver.erp_transactions (
					txn_id,
					user_id,
					txn_date,
					amount,
					currency,
					payment_method,
					merchant_id,
					status,
					fraud_flag,
					dwh_create_time
				)
				SELECT
					txn_id,
					user_id,
					txn_date,
					amount,
					currency,
					payment_method,
					merchant_id,
					status,
					fraud_flag,
					GETDATE()
				FROM (
					SELECT
						t.txn_id,
						t.user_id,
						t.txn_date,        
						TRY_CONVERT(DECIMAL(18,2), t.amount) AS amount,        
						UPPER(TRIM(t.currency)) AS currency,
						TRIM(t.payment_method) AS payment_method,
						LOWER(TRIM(t.status)) AS status,
						t.merchant_id,        
						CASE WHEN t.fraud_flag = 1 THEN 1 ELSE 0 END AS fraud_flag,
						ROW_NUMBER() OVER (PARTITION BY t.txn_id ORDER BY t.txn_date DESC) AS rn
					FROM bronze.erp_transactions t    
					INNER JOIN silver.crm_users u
						ON t.user_id = u.user_id
					INNER JOIN silver.erp_merchants m
						ON t.merchant_id = m.merchant_id
					WHERE t.txn_id IS NOT NULL
				) t
				WHERE rn = 1
				AND amount IS NOT NULL
				AND amount > 0
				AND status IN ('success','failed','refunded')
				AND txn_date IS NOT NULL
				AND txn_date <= GETDATE();
				SET @end_time = GETDATE();
				PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
				PRINT('---------------------')
				----------------------------------------------------------------------------------------------------------------------
				--Trunckate table before insert (Full Load)
				SET @start_time = GETDATE();
				PRINT('---------------------')
				PRINT('>>> Truncating Table: silver.erp_fees')
				TRUNCATE TABLE silver.erp_fees;
				--Insert data in bronze.crm_users table
				PRINT('>>> Inserting Data Into: silver.erp_fees')
				INSERT INTO silver.erp_fees (
					txn_id,
					processing_fee,
					service_fee,
					fx_fee,
					total_fee,
					dwh_create_time
				)
				SELECT
					txn_id,
					processing_fee,
					service_fee,
					fx_fee,
					processing_fee + service_fee + fx_fee AS total_fee,
					GETDATE()
				FROM (
					SELECT
						f.txn_id,        
						ISNULL(TRY_CONVERT(DECIMAL(18,2), f.processing_fee), 0) AS processing_fee,
						ISNULL(TRY_CONVERT(DECIMAL(18,2), f.service_fee), 0) AS service_fee,
						ISNULL(TRY_CONVERT(DECIMAL(18,2), f.fx_fee), 0) AS fx_fee,
						ROW_NUMBER() OVER ( PARTITION BY f.txn_id ORDER BY f.txn_id ) AS rn
					FROM bronze.erp_fees f	  
					INNER JOIN silver.erp_transactions t
						ON f.txn_id = t.txn_id

					WHERE f.txn_id IS NOT NULL
				) t
				WHERE rn = 1;
				SET @end_time = GETDATE();
				PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
				PRINT('---------------------')
				----------------------------------------------------------------------------------------------------------------------
				--Trunckate table before insert (Full Load)
				SET @start_time = GETDATE();
				PRINT('---------------------')
				PRINT('>>> Truncating Table: silver.erp_fraud_signals')
				TRUNCATE TABLE silver.erp_fraud_signals;
				--Insert data in bronze.crm_users table
				PRINT('>>> Inserting Data Into: silver.erp_fraud_signals')
				INSERT INTO silver.erp_fraud_signals (
					txn_id,
					ip_address,
					location,
					device_id,
					risk_score,
					dwh_create_time
				)
				SELECT
					txn_id,
					ip_address,
					location,
					device_id,
					risk_score,
					GETDATE()
				FROM (
					SELECT
						f.txn_id,
						TRIM(f.ip_address) AS ip_address,
						UPPER(TRIM(f.location)) AS location,
						TRIM(f.device_id) AS device_id,        
						TRY_CONVERT(DECIMAL(5,3), f.risk_score) AS risk_score,
						ROW_NUMBER() OVER (PARTITION BY f.txn_id ORDER BY f.risk_score DESC ) AS rn
					FROM bronze.erp_fraud_signals f    
					INNER JOIN silver.erp_transactions t
						ON f.txn_id = t.txn_id
					WHERE f.txn_id IS NOT NULL
					) t
				WHERE rn = 1
				AND risk_score IS NOT NULL
				AND risk_score BETWEEN 0 AND 1;
				SET @end_time = GETDATE();
				PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
				PRINT('---------------------')
				----------------------------------------------------------------------------------------------------------------------
				--Trunckate table before insert (Full Load)
				SET @start_time = GETDATE();
				PRINT('---------------------')
				PRINT('>>> Truncating Table: silver.erp_refunds')
				TRUNCATE TABLE silver.erp_refunds;
				--Insert data in bronze.crm_users table
				PRINT('>>> Inserting Data Into: silver.erp_refunds')
				INSERT INTO silver.erp_refunds (
					refund_id,
					txn_id,
					refund_amount,
					refund_date,
					reason,
					dwh_create_time
				)
				SELECT
					refund_id,
					txn_id,
					refund_amount,
					refund_date,
					reason,
					GETDATE()
				FROM (
						SELECT
							r.refund_id,
							r.txn_id,        
							TRY_CONVERT(DECIMAL(18,2), r.refund_amount) AS refund_amount,
							r.refund_date,
							TRIM(r.reason) AS reason,
							ROW_NUMBER() OVER (PARTITION BY r.refund_id ORDER BY r.refund_date DESC) AS rn,
							t.amount AS txn_amount,
							t.txn_date
						FROM bronze.erp_refunds r 
						INNER JOIN silver.erp_transactions t
							ON r.txn_id = t.txn_id
						WHERE r.refund_id IS NOT NULL
					) t
				WHERE rn = 1
				AND refund_amount IS NOT NULL
				AND refund_amount > 0
				AND refund_amount <= txn_amount
				AND refund_date IS NOT NULL
				AND refund_date <= GETDATE()
				AND refund_date >= txn_date
				SET @end_time = GETDATE();
				PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
				PRINT('---------------------')
			----------------------------------------------------------------------------------------------------------------------
		END TRY

		BEGIN CATCH
			PRINT('------------------------------------------------')
			PRINT('ERROR OCCURED DURING LOADING BRONZE LAYER')
			PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
			PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT('------------------------------------------------')
		END CATCH
		SET @batch_end_time = GETDATE();
		PRINT('================================')
		PRINT('Loading Bronze Layer Completed')
		PRINT '>> Loading Duration ETL: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT('================================')	
END
