/*
========================================================================================================================================================================
Create database & Schemas
========================================================================================================================================================================
Script Purpose:
This script creates a new database named 'Datawarehouse' after checking if it already exists.
If the database exists,it is dropped & recreated.Additionally,the script sets up three schemas within the database:'bronze','silver','gold'.

Warning:
running this script will drop the entire 'Datawarehouse' database if it exists.
All data in the database will be permanently deleted.Proceed with caution & ensure you have proper backups before running this script.
*/

use master
go
--Drop & recreate the 'Datawarehouse' database
If exists(select 1 from sys.databases where name='Datawarehouse')
begin
  alter database Datawarehouse set single user with rollback immediate
  drop database Datawarehouse
end
go
--Create database Datawarehouse
create database Datawarehouse
go
use Datawarehouse
go

--Create Schemas
create schema bronze
go /* Go separate batches while working with multiple SQL statements*/ 
create schema silver
go
create schema gold
