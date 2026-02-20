/*
===================================================================================================
DDL Script: Create Bronze Tables
===================================================================================================
Script Purpose:
           This script creates tables in the 'bronze' schema, dropping existing tables if they already exists.
           Run this script to re-define the DDL structure of 'bronze' tables
====================================================================================================
*/

if OBJECT_ID('bronze.crm_cust_info','u') is not null
drop table bronze.crm_cust_info
go

create table bronze.crm_cust_info(
			cst_id int,
			cst_key nvarchar(50),
			cst_firstname nvarchar(50),
			cst_lastname nvarchar(50),
			cst_marital_status nvarchar(50),
			cst_gndr nvarchar(50),
			cst_create_date date)
go
if OBJECT_ID('bronze.crm_prd_info','u') is not null
drop table bronze.crm_prd_info
go

create table bronze.crm_prd_info(
			prd_id int,
			prd_key nvarchar(50),
			prd_nm nvarchar(50),
			prd_cost int,
			prd_line nvarchar(50),
			prd_start_dt date,
			prd_end_dt date)
go
if OBJECT_ID('bronze.crm_sales_details','u') is not null
drop table bronze.crm_sales_details
go
create table bronze.crm_sales_details(
			sls_ord_num nvarchar(50),
			sls_prd_key nvarchar(50),
			sls_cust_id int,
			sls_order_dt int,
			sls_ship_dt int,
			sls_due_dt int,
			sls_sales int,
			sls_quantity int,
			sls_price int)
go
/*creating bronze layer tables for erp files*/

if OBJECT_ID('bronze.erp_cust_AZ12','u') is not null
drop table bronze.erp_cust_AZ12
go
create table bronze.erp_cust_AZ12(
			CID nvarchar(50),
			BDATE date,
			GEN nvarchar(50))
go
if OBJECT_ID('bronze.erp_LOC_A101','u') is not null
drop table bronze.erp_LOC_A101
go
create table bronze.erp_LOC_A101(
			CID nvarchar(50),
			CNTRY nvarchar(50))
go
if OBJECT_ID('bronze.erp_PX_CAT_G1V2','u') is not null
drop table bronze.erp_PX_CAT_G1V2
go
create table bronze.erp_PX_CAT_G1V2(
			ID nvarchar(50),
			CAT nvarchar(50),
			SUBCAT nvarchar(50),
			MAINTENANCE nvarchar(50))
go
/*--------------------------------------------------------------------------------------------------------------------------------------------------------------*/
/*From here we will insert the data into different bronze tables & for inserting the data we'll follow the bulk insert method or full load*/
/* as we are following truncate & insert method we have to first empty the table by using truncate & then insert the data using bulk insert*/
/*Inserting data into source_crm-Cust_info*/
/*we will create a stored procedure so that we don't have to run each & every query for seeing the output of every table in the bronze layer*/
create or alter procedure bronze.load_bronze As
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
begin try
set @batch_start_time=GETDATE()
	print '================================='
	print 'Loading bronze Layer'
	print '================================='
	print '---------------------------------'
	print 'Loading CRM tables'
	print '---------------------------------'
set @start_time=GETDATE()

    print '>> Truncating table:bronze.crm_cust_info' 
truncate table bronze.crm_cust_info
    print '>> Inserting data into:bronze.crm_cust_info'

/* we have to give the full file path here otherwise it won't work, added.csv after coping the full path from folder*/
bulk insert bronze.crm_cust_info 
from 'C:\Users\raksh\OneDrive\Data Warehouse Project\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with(
      firstrow = 2,/*this is for specifying that from which row data needs to be loaded as we already created the table & mentioned the headers so we'll start loading             from 2nd row*/
      fieldterminator = ',',/*we have to mention the delimiter which we have in the raw data so that it is clear while loading*/
      tablock/* this locks the whole table while loading so that no changes can be done during that time*/
)
set @end_time=GETDATE()
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print '>>-----------'

/*commenting out this so that the output is clear but we can check the quality by this-select * from bronze.crm_cust_info*/
/* after bulk inserting the data checking the quality such as whether all the data is inserted into correct columns*/
/*select count(*) from bronze.crm_cust_info checking all data has been uploaded or not*/


/*2nd table______________________________________________________________________________________________________________________________________________*/
/*prd_info*/
set @start_time=GETDATE()
	print '>> Truncating table:bronze.crm_prd_info' 
truncate table bronze.crm_prd_info
	print '>> Inserting data into:bronze.crm_prd_info'

bulk insert bronze.crm_prd_info from 'C:\Users\raksh\OneDrive\Data Warehouse Project\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' 
with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
)
set @end_time=GETDATE()
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print '>>-----------'


/*select * from bronze.crm_prd_info
select count(*) from bronze.crm_prd_info*/

/*3rd table_____________________________________________________________________________________________________________________________________________________*/
/*sales_details*/
set @start_time=GETDATE()
	print '>> Truncating table:bronze.crm_sales_details' 
truncate table bronze.crm_sales_details
	print '>> Inserting data into:bronze.crm_sales_details'

bulk insert bronze.crm_sales_details from 'C:\Users\raksh\OneDrive\Data Warehouse Project\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' with(
firstrow = 2,
fieldterminator = ',',
tablock)
set @end_time=GETDATE()
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print '>>-----------'

/*select * from bronze.crm_sales_details
select count(*) from bronze.crm_sales_details*/

/*____________________________________________________________________________________________________________________________________________________*/

/*Inserting data into source_erp*/
/*CUST_AZ12*/
set @start_time=GETDATE()
	print '---------------------------------'
	print 'Loading ERP tables'
	print '---------------------------------'
	print '>> Truncating table:bronze.erp_cust_AZ12'
truncate table bronze.erp_cust_AZ12
	print '>> Inserting data into:bronze.erp_cust_AZ12'

bulk insert bronze.erp_cust_AZ12 from 'C:\Users\raksh\OneDrive\Data Warehouse Project\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' with(
firstrow = 2,
fieldterminator = ',',
tablock)
set @end_time=GETDATE()
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print '>>-----------'

/*select * from bronze.erp_cust_AZ12
select count(*) from bronze.erp_cust_AZ12*/

/*2nd table____________________________________________________________________________________________________________________________________________________*/
/*LOC_A101*/
set @start_time=GETDATE()
	print '>> Truncating table:bronze.erp_LOC_A101'
truncate table bronze.erp_LOC_A101
	print '>> Inserting data into:bronze.erp_LOC_A101'

bulk insert bronze.erp_LOC_A101 from 'C:\Users\raksh\OneDrive\Data Warehouse Project\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' with(
firstrow = 2,
fieldterminator = ',',
tablock)
set @end_time=GETDATE()
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print '>>-----------'

/*select * from bronze.erp_LOC_A101
select count(*) from bronze.erp_LOC_A101*/

/*3rd table_______________________________________________________________________________________________________________________________________________*/
/*PX_CAT_G1V2*/
set @start_time=GETDATE()
	print '>> Truncating table:bronze.erp_PX_CAT_G1V2'
truncate table bronze.erp_PX_CAT_G1V2
	print '>> Inserting data into:bronze.erp_PX_CAT_G1V2'

bulk insert bronze.erp_PX_CAT_G1V2 from 'C:\Users\raksh\OneDrive\Data Warehouse Project\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' with(
firstrow = 2,
fieldterminator = ',',
tablock)
set @end_time=GETDATE()
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print '>>-----------'
set @batch_end_time=GETDATE()
	print '==================================='
	print 'Loading bronze layer is completed'
	print 'Total load duration: '+cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar)+'seconds'
	print '==================================='
end try
begin catch
	print '===================================='
	print 'Error Occured during loading the bronze layer'
	print 'Error Message' + Error_Message()
	print 'Error Message' + cast(Error_Number() as Nvarchar)
	print 'Error Message' + cast(Error_State() as Nvarchar)
	print '===================================='
end catch

end

exec bronze.load_bronze
