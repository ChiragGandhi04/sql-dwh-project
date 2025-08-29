/*

=================================
Create Database and Schemas
=================================
Script Purpose:
  This script create a new database name 'DataWarehouse' after checking if it already exisits.
  If the database already exisits, it is dropped and recreate. Additionally, the script sets up three schemas  
  within the database: 'bronze', 'silver', and 'gold'.

warning:
  Running this script will drop the entire 'DataWarehouse' database if it exisits.
  All data in the database will be permently deleted. Proceed with caution and 
  ensure you have proper backups before running this script.
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

USE DataWarehouse;


--GO- separate batches when working with multiple SQL statements
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
