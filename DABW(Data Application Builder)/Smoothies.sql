---Create a FRUIT_OPTIONS Table
CREATE TABLE SMOOTHIES.PUBLIC.FRUIT_OPTIONS(
    FRUIT_ID INT,
    FRUIT_NAME varchar(25)
);
----Download the TXT File of Fruit Names 

--Create A FILE FORMAT to Load the Fruit File
create or replace file format SMOOTHIES.PUBLIC.two_headerrow_pct_delim 
  type = 'CSV' --use CSV for any flat file
  compression = 'AUTO' 
  field_delimiter = '%' --pipe or vertical bar
  skip_header = 2  --1 header row
  trim_space = FALSE;

--Create an internal Stage to Load the file into it

--Query the Not-Yet-Loaded Data Using the File Format
SELECT $1, $2
  FROM @SMOOTHIES.PUBLIC.MY_UPLOADED_FILES/fruits_available_for_smoothies.txt
  (FILE_FORMAT => SMOOTHIES.PUBLIC.two_headerrow_pct_delim);

--Reorder Columns During the COPY INTO LOAD as the columns are in reverse order
SELECT $1 as FRUIT_NAME, $2 as FRUIT_ID
  FROM @SMOOTHIES.PUBLIC.MY_UPLOADED_FILES/fruits_available_for_smoothies.txt
  (FILE_FORMAT => SMOOTHIES.PUBLIC.two_headerrow_pct_delim);

--Reorder Columns During the COPY INTO LOAD as the columns are in reverse order
copy into SMOOTHIES.PUBLIC.FRUIT_OPTIONS
from (
  SELECT $2 as FRUIT_ID, $1 as FRUIT_NAME
  from @SMOOTHIES.PUBLIC.MY_UPLOADED_FILES/fruits_available_for_smoothies.txt)
  file_format = (format_name = SMOOTHIES.PUBLIC.two_headerrow_pct_delim)
on_error = abort_statement
purge = true
;

--create table orders
create table SMOOTHIES.PUBLIC.ORDERS
(
ingredients varchar (200)
);

--insert into order table 
insert into SMOOTHIES.PUBLIC.ORDERS(ingredients) values ('BlueberriesDragon Fruit')

select * from SMOOTHIES.PUBLIC.ORDERS;

--truncate the table
truncate table SMOOTHIES.PUBLIC.ORDERS;

--Alter order table to add a new column name_on_order
alter table SMOOTHIES.PUBLIC.ORDERS
add column name_on_order varchar(100);

--Add a column named ORDER_FILLED to the ORDERS table
alter table SMOOTHIES.PUBLIC.ORDERS
add column ORDER_FILLED boolean DEFAULT FALSE;

--update orders created before adding NAME_ON_ORDER
update SMOOTHIES.PUBLIC.ORDERS
set ORDER_FILLED = true
where name_on_order is null;

--Create a squence to use as a row id
create sequence SMOOTHIES.PUBLIC.order_seq
    start = 1
    increment = 2
    ORDER
    comment = 'Provide a unique id for each smoothie order'; 
    
--Add the Unique ID Column
alter table SMOOTHIES.PUBLIC.ORDERS
add column order_uid  integer 
default SMOOTHIES.PUBLIC.order_seq.nextval --sets the value of the column to sequence
constraint order_uid unique enforced; --makes sure there is always a unique value in the column

-- Orders table definition is altered multiple times
-- lets drop and recreate the table
create or replace table SMOOTHIES.PUBLIC.ORDERS
(
order_uid  integer 
default SMOOTHIES.PUBLIC.order_seq.nextval,
ingredients varchar (200),
name_on_order varchar(100),
ORDER_FILLED boolean DEFAULT FALSE,
constraint order_uid unique (order_uid),
order_ts timestamp_ltz default current_timestamp()
);

--Add new column SEARCH_ON to fruit_option table
ALTER TABLE SMOOTHIES.PUBLIC.FRUIT_OPTIONS
add column SEARCH_ON varchar(200);

-- Update fruit option table serach_on column
UPDATE  SMOOTHIES.PUBLIC.ORDERS
set NAME_ON_ORDER = 'Xi'
where NAME_ON_ORDER = 'Xi ';

select * from SMOOTHIES.PUBLIC.FRUIT_OPTIONS;