/*
========================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
========================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSERT' Command to load data from csv Files to bronze tables.

Parameters:
    None.
  This stored procedures does not accept any parameters or return any values.

Usage Example:
  EXEC bronce.load_bronze;

*/



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
