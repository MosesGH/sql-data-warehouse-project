/*
====================================================
Create Database and Schemas
====================================================
Script Purpose:
	This script creates a new database namesd 'DataWarehouse' after checking if it already exists. 
	If the database exists, it is dropped and recreated. Additionally, the script sets up three 
	schemas with the database: 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists. 
	All the data in the database will be permanently deleted. Proceed with caution
	and ensure yoiu have proper backups before running this script. 
*/

--Create Database 'DataWarehouse
USE master;
GO

--Drop a recreate the ' DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse') -- checking for if the database already eists
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;	-- Means that all current transactions are Rolled back (the database is reset to the point where
																			-- the transaction started)
	DROP DATABASE DataWarehouse
END;
go

--Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse
GO

USE DataWarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO --seperates batches when wirking with multiple SQL statements in many microsoft applications
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO


