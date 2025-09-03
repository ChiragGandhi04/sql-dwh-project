/*
====================================================================================================
DDL Script: Create Bronze Tables
=====================================================================================================
Script Purpose:
  This script creates tables in the 'bronze' schema, dropping existing tables if they already exists.
  Run this script to re-define the DDL structure of 'bronze' Tables
=======================================================================================================
*/



-- Create Database 'DataWarehouse'

USE master;

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse' )
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse
END;
GO

CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;



--GO- separate batches when working with multiple SQL statements
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO


----Bronze 
--FIRST THE SYSTEM CHECK WHEATHER TABLE IS PRESENT OR NOT IF YES THEN SYSTEM WILL DROP EXISTENCE TABLE AND CREATE NEW TABLE WITH THE FORMAT 
--GIVEN BELOW
IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL 
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
cst_id INT,
cst_key	NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),	
cst_marital_status NVARCHAR(50),	
cst_gndr NVARCHAR(50),	
cst_create_date DATE
);
IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL 
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key	NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,	
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL 
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
sls_ord_num NVARCHAR(50),	
sls_prd_key NVARCHAR(50),	
sls_cust_id	INT,
sls_order_dt INT,	
sls_ship_dt	INT,
sls_due_dt INT,	
sls_sales INT,	
sls_quantity INT, 	
sls_price INT
);

--ERP
IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL 
	DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL 
	DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
cid NVARCHAR(50),
cntry NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL 
	DROP TABLE bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);


/*
Insert the data into the table using 
bulk insert
*/
-- everytime when you load a data into table its a good habit to do truncate 
-- remove all old data if present and load new data this habit maintane good data quality
CREATE OR ALTER PROCEDURE bronze.load_bronze AS -- this cmd is used to do stored procedures if we have do this task on regular basis.
BEGIN
	-- TRACK THE ETL DURATION SO AS A DATA ENGINEER CAN FIGURE OUT WHICH CMD TAKE LOTS OF TIME 
	-- ALSO BELOW CMD IS declaring the variable which will used track time duration 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	BEGIN TRY --adding try and catch so we can handle error, data integrity, and issue logging for easier debugging
	--always add this kind of print statement because it help to understsand the flow
		SET @batch_start = GETDATE();
		print'============================================================================'; 
		print'Loading Bronze Layer';
		print'============================================================================';

		print'----------------------------------------------------------------------------';
		print'Loading CRM Tables';
		
		
		SET @start_time = GETDATE();
		print'>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		print'>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print'>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; -- calculate the duration using DATEDIFF
		print'-------------------------------------';
		
		SET @start_time = GETDATE();
		print'>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
	
		print'>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print'>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'-------------------------------------';

		SET @start_time = GETDATE();
		print'>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		print'>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print'>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'-------------------------------------';

		print'----------------------------------------------------------------------------';
		print'Loading CRM Tables';
		
		SET @start_time = GETDATE();
		print'>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		print'>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print'>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'-------------------------------------';

		SET @start_time = GETDATE();
		print'>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		print'>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print'>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'-------------------------------------';

		SET @start_time = GETDATE();
		print'>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		print'>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print'>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print'-------------------------------------';

		SET @batch_end = GETDATE();
		print'===============================================';
		print'Loading Bronze layer completed';
		print'  -Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR) + 'seconds';
		print'==============================================='
	END TRY
	BEGIN CATCH
		PRINT'===============================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'===============================================';
	END CATCH
end


-- to execute the stored procedure then we have to run below cmd
EXEC bronze.load_bronze
