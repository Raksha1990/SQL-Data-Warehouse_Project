/*
=====================================================================================================
Stored Procedure: Load Bronze Layer (Source->Bronze )
=====================================================================================================
Script Purpose:
       This stored procedure loads data into the 'bronze' schema from external CSV files.
       It performs the following actions:
       - Truncates the bronze tables before loading data.
       - Uses the 'Bulk Insert' command to load data from CSV files to bronze tables.

Parameters:
       None.
       This stored procedure does not accept any parameters or return any values.

Usage example:
       Exec bronze.load_bronze

=====================================================================================================
*/

create or alter procedure bronze.load_bronze As
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
begin try
set @batch_start_time=GETDATE()
	print '================================='
	print 'Loading Bronze Layer'
	print '================================='
	print '---------------------------------'
	print 'Loading CRM tables'
	print '---------------------------------'
set @start_time=GETDATE()

    print '>> Truncating table:bronze.crm_cust_info' 
truncate table bronze.crm_cust_info
    print '>> Inserting data into:bronze.crm_cust_info'

bulk insert bronze.crm_cust_info from 'C:\Users\raksh\OneDrive\Data Warehouse Project\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' with(
firstrow = 2,
fieldterminator = ',',
tablock)

set @end_time=GETDATE()
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print '>>-----------'

select * from bronze.crm_cust_info
/*______________________________________________________________________________________________________________________________________________________________*/
/*prd_info*/
set @start_time=GETDATE()
	print '>> Truncating table:bronze.crm_prd_info' 
truncate table bronze.crm_prd_info
	print '>> Inserting data into:bronze.crm_prd_info'

bulk insert bronze.crm_prd_info from 'C:\Users\raksh\OneDrive\Data Warehouse Project\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' with(
firstrow = 2,
fieldterminator = ',',
tablock)
set @end_time=GETDATE()
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print '>>-----------'


select * from bronze.crm_prd_info

/*______________________________________________________________________________________________________________________________________________________________*/
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

select * from bronze.crm_sales_details

/*______________________________________________________________________________________________________________________________________________________________*/

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

select * from bronze.erp_cust_AZ12

/*______________________________________________________________________________________________________________________________________________________________*/
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

select * from bronze.erp_LOC_A101

/*______________________________________________________________________________________________________________________________________________________________*/
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
	print 'Loading Bronze layer is completed'
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
