use master
drop database Datawarehouse
create database Datawarehouse
use Datawarehouse
create schema bronze
go /* Go separate batches while working with multiple SQL statements*/ 
create schema silver
go
create schema gold
