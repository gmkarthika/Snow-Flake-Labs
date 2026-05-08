--Create database and schema 
CREATE DATABASE VIN;
CREATE SCHEMA VIN.DECODE;

--We need a table that will allow WMIs to be decoded into Manufacturer Name, Country and Vehicle Type
CREATE TABLE vin.decode.wmi_to_manuf (
    wmi varchar(6),
    manuf_id number(6),
    manuf_name varchar(50),
    country varchar(50),
    vehicle_type varchar(50)
);
--We need a table that will allow to go from Manufacturer to Make
--For example, Mercedes AG of Germany and Mercedes USA both roll up into Mercedes
--But they use different WMI Codes
CREATE TABLE vin.decode.manuf_to_make (
    manuf_id number(6),
    make_name varchar(50),
    make_id number(5)
);
--We need a table that can decode the model year
-- The year 2001 is represented by the digit 1
-- The year 2020 is represented by the letter L
CREATE TABLE vin.decode.model_year (
    model_year_code varchar(1),
    model_year_name varchar(4)
);
--We need a table that can decode which plant at which
--the vehicle was assembled
-- code "A" for Honda and code "A" for Ford
--so we need both the Make and the Plant Code to properly decode
--the plant code
CREATE TABLE vin.decode.manuf_plants (
    make_id number(5),
    plant_code varchar(1),
    plant_name varchar(75)
);
--We need to use a combination of both the Make and VDS
--to decode many attributes including the engine, transmission, etc
CREATE TABLE vin.decode.make_model_vds (
    make_id number(3),
    model_id number(6),
    model_name varchar(50),
    vds varchar(5),
    desc1 varchar(25),
    desc2 varchar(25),
    desc3 varchar(50),
    desc4 varchar(25),
    desc5 varchar(25),
    body_style varchar(25),
    engine varchar(100),
    drive_type varchar(50),
    transmission varchar(50),
    mpg varchar(25)
);
--Create a file format and then load each of the 5 Lookup Tables
CREATE FILE FORMAT vin.decode.comma_sep_oneheadrow 
    type = 'CSV' field_delimiter = ',' 
    record_delimiter = '\n' 
    skip_header = 1 
    field_optionally_enclosed_by = '"' 
    trim_space = TRUE;

--Load the Tables and Check Out the Data
COPY INTO vin.decode.wmi_to_manuf
from
    @vin.decode.aws_s3_bucket files = ('Maxs_WMIToManuf_data.csv') 
    file_format =(format_name = vin.decode.comma_sep_oneheadrow);
    
COPY INTO vin.decode.manuf_to_make
from
    @vin.decode.aws_s3_bucket files = ('Maxs_ManufToMake_Data.csv') 
    file_format =(format_name = vin.decode.comma_sep_oneheadrow);
    
COPY INTO vin.decode.model_year
from
    @vin.decode.aws_s3_bucket files = ('Maxs_ModelYear_Data.csv') 
    file_format =(format_name = vin.decode.comma_sep_oneheadrow);
    
COPY INTO vin.decode.manuf_plants
from
    @vin.decode.aws_s3_bucket files = ('Maxs_ManufPlants_Data.csv') 
    file_format =(format_name = vin.decode.comma_sep_oneheadrow);
    
COPY INTO vin.decode.make_model_vds
from
    @vin.decode.aws_s3_bucket files = ('Maxs_MMVDS_Data.csv') 
    file_format =(format_name = vin.decode.comma_sep_oneheadrow);
    
--we can use a list command to see the names of the files in the stage
list @vin.decode.aws_s3_bucket;

--------------------------------------
--Parsing a VIN Into Its Important Parts
--------------------------------------
--create a variable and set the value
set
    sample_vin = 'SAJAJ4FX8LCP55916';
--check to make sure variable above is set
select
    $sample_vin;
--parse the vin into it's important pieces
select
    $sample_vin as VIN,
    LEFT($sample_vin, 3) as WMI,
    SUBSTR($sample_vin, 4, 5) as VDS,
    SUBSTR($sample_vin, 10, 1) as model_year_code,
    SUBSTR($sample_vin, 11, 1) as plant_code;

--------------------------------------------------
--A Parsed VIN that Returns Lots of Information
--------------------------------------------------
select
    VIN,
    manuf_name,
    vehicle_type,
    make_name,
    plant_name,
    model_year_name as model_year,
    model_name,
    desc1,
    desc2,
    desc3,
    desc4,
    desc5,
    engine,
    drive_type,
    transmission,
    mpg
from
    (
        select
            $sample_vin as VIN,
            LEFT($sample_vin, 3) as WMI,
            SUBSTR($sample_vin, 4, 5) as VDS,
            SUBSTR($sample_vin, 10, 1) as model_year_code,
            SUBSTR($sample_vin, 11, 1) as plant_code
    ) vin
    JOIN vin.decode.wmi_to_manuf w ON w.WMI = vin.WMI
    JOIN vin.decode.manuf_to_make m ON m.MANUF_ID = w.manuf_id
    JOIN vin.decode.manuf_plants p ON p.PLANT_CODE = vin.PLANT_CODE
    AND p.make_id = m.MAKE_ID
    JOIN vin.decode.model_year y ON y.MODEL_YEAR_CODE = vin.MODEL_YEAR_CODE
    JOIN vin.decode.make_model_vds vds ON vds.VDS = vin.VDS
    AND vds.MAKE_ID = m.MAKE_ID;
    
--Create User Defined Table Function
create secure function parse_and_enhance_vin(this_vin varchar(25)) 
returns table (
        VIN varchar(25),
        manuf_name varchar(25),
        vehicle_type varchar(25),
        make_name varchar(25),
        plant_name varchar(25),
        model_year varchar(25),
        model_name varchar(25),
        desc1 varchar(25),
        desc2 varchar(25),
        desc3 varchar(25),
        desc4 varchar(25),
        desc5 varchar(25),
        engine varchar(25),
        drive_type varchar(25),
        transmission varchar(25),
        mpg varchar(25)
    ) as $$
select VIN
, manuf_name
, vehicle_type
, make_name
, plant_name
, model_year_name as model_year
, model_name
, desc1
, desc2
, desc3
, desc4
, desc5
, engine
, drive_type
, transmission
, mpg
from
    (select this_vin as VIN,
        LEFT(this_vin,3) as WMI,
        SUBSTR(this_vin,4,5) as VDS,
        SUBSTR(this_vin,10,1) as model_year_code,
        SUBSTR(this_vin,11,1) as plant_code
    ) vin 
JOIN vin.decode.wmi_to_manuf w
    ON w.WMI = vin.WMI
JOIN vin.decode.manuf_to_make m
    ON m.MANUF_ID = w.manuf_id
JOIN vin.decode.manuf_plants p
    ON p.PLANT_CODE = vin.PLANT_CODE
    AND p.make_id = m.MAKE_ID
JOIN vin.decode.model_year y
    ON y.MODEL_YEAR_CODE = vin.MODEL_YEAR_CODE
JOIN vin.decode.make_model_vds vds
    ON vds.VDS = vin.VDS
    AND vds.MAKE_ID = m.MAKE_ID

$$;

--In each function call below, we pass in a different VIN as THIS_VIN
select
    *
from
    table (
        vin.decode.parse_and_enhance_vin('SAJAJ4FX8LCP55916')
    );

--------------------------------------------------
--Create a table that stores the ACME Car Inventory
--------------------------------------------------
create or replace table stock.unsold.lotstock
(
  vin varchar(25)
, exterior varchar(50)	
, interior varchar(50)
, manuf_name varchar(25)
, vehicle_type varchar(25)
, make_name varchar(25)
, plant_name varchar(25)
, model_year varchar(25)
, model_name varchar(25)
, desc1 varchar(25)
, desc2 varchar(25)
, desc3 varchar(25)
, desc4 varchar(25)
, desc5 varchar(25)
, engine varchar(25)
, drive_type varchar(25)
, transmission varchar(25)
, mpg varchar(25)
);

--Query the file in stage(aws s3 stage) before loading it 

select $1, $2, $3
from @stock.unsold.aws_s3_bucket/Lotties_LotStock_Data.csv;

list @stock.unsold.aws_s3_bucket;

--Create a File Format for ACME
--Create a file format and then load each of the 5 Lookup Tables
CREATE FILE FORMAT CSV_COMMA_LF_HEADER 
type = 'CSV' 
field_delimiter = ',' 
record_delimiter = '\n' 
skip_header = 1 
field_optionally_enclosed_by = '"'  
trim_space = TRUE;

--Query the File Again, with the Help of a File Format
select $1, $2, $3
from @stock.unsold.aws_s3_bucket/Lotties_LotStock_Data.csv
(file_format => util_db.public.csv_comma_lf_header);

--How to Load a File of 3 Columns into a Table of 18 Columns?
--create a new file format
CREATE FILE FORMAT util_db.public.CSV_COL_COUNT_DIFF 
type = 'CSV' 
field_delimiter = ',' 
record_delimiter = '\n' 
parse_header  = TRUE
error_on_column_count_mismatch = FALSE
field_optionally_enclosed_by = '"'  
trim_space = TRUE;

-- With a parsed header, Snowflake can MATCH BY COLUMN NAME during the COPY INTO
copy into stock.unsold.lotstock
from @stock.unsold.aws_s3_bucket/Lotties_LotStock_Data.csv
file_format = (format_name = util_db.public.csv_col_count_diff)
match_by_column_name='CASE_INSENSITIVE';

--View the Table and Its Contents
select * from stock.unsold.lotstock
where vin = '5J8YD4H86LL013641';


--A simple select from Lot Stock (choose any VIN from the LotStock table)
select * 
from stock.unsold.lotstock
where vin = '5J8YD4H86LL013641';

-- here we use ls for lotstock table and pf for parse function
-- this more complete statement lets us combine the data already in the table 
-- with the data returned from the parse function
select ls.vin, ls.exterior, ls.interior, pf.*
from
(select * 
from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN('5J8YD4H86LL013641'))
) pf
join stock.unsold.lotstock ls
where pf.vin = ls.vin;
;

-- We can use a local (session) variable to make it easier to change the VIN we are trying to enhance
set my_vin = '5J8YD4H86LL013641';

SAJAJ4FX8LCP55916
3MW5R7J0XL8B20091
WBAHF9C01LWW35390

select $my_vin;
select ls.vin, pf.manuf_name, pf.vehicle_type
        , pf.make_name, pf.plant_name, pf.model_year
        , pf.desc1, pf.desc2, pf.desc3, pf.desc4, pf.desc5
        , pf.engine, pf.drive_type, pf.transmission, pf.mpg
from stock.unsold.lotstock ls
join 
    (   select 
          vin, manuf_name, vehicle_type
        , make_name, plant_name, model_year
        , desc1, desc2, desc3, desc4, desc5
        , engine, drive_type, transmission, mpg
        from table(VIN.DECODE.PARSE_AND_ENHANCE_VIN($my_vin))
    ) pf
on pf.vin = ls.vin;

-- We're using "s" for "source." The joined data from the LotStock table and the parsing function will be a source of data for us. 
-- We're using "t" for "target." The LotStock table is the target table we want to update.

set my_vin = 'SAJAJ4FX8LCP55916';
set my_vin = '3MW5R7J0XL8B20091';
set my_vin = 'WBAHF9C01LWW35390';
 
update stock.unsold.lotstock t
set manuf_name = s.manuf_name
, vehicle_type = s.vehicle_type
, make_name = s.make_name
, plant_name = s.plant_name
, model_year = s.model_year
, desc1 = s.desc1
, desc2 = s.desc2
, desc3 = s.desc3
, desc4 = s.desc4
, desc5 = s.desc5
, engine = s.engine
, drive_type = s.drive_type
, transmission = s.transmission
, mpg = s.mpg
from 
(
    select ls.vin, pf.manuf_name, pf.vehicle_type
        , pf.make_name, pf.plant_name, pf.model_year
        , pf.desc1, pf.desc2, pf.desc3, pf.desc4, pf.desc5
        , pf.engine, pf.drive_type, pf.transmission, pf.mpg
    from stock.unsold.lotstock ls
    join 
    (   select 
          vin, manuf_name, vehicle_type
        , make_name, plant_name, model_year
        , desc1, desc2, desc3, desc4, desc5
        , engine, drive_type, transmission, mpg
        from table(VIN.DECODE.PARSE_AND_ENHANCE_VIN($my_vin))
    ) pf
    on pf.vin = ls.vin
) s
where t.vin = s.vin;

-- We can count the number of rows in the LotStock table that have not yet been updated.  
 
set row_count = (select count(*) 
                from stock.unsold.lotstock
                where manuf_name is null);

select $row_count;

select * from stock.unsold.lotstock
where vin = 'SAJAJ4FX8LCP55916'

select count(*) 
from stock.unsold.lotstock

-- This scripting block runs very slow, but it shows how blocks work 
DECLARE
    update_stmt varchar(2000);
    res RESULTSET;
    cur CURSOR FOR select vin from stock.unsold.lotstock where manuf_name is null;
BEGIN
    OPEN cur;
    FOR each_row IN cur DO
        update_stmt := 'update stock.unsold.lotstock t '||
            'set manuf_name = s.manuf_name ' ||
            ', vehicle_type = s.vehicle_type ' ||
            ', make_name = s.make_name ' ||
            ', plant_name = s.plant_name ' ||
            ', model_year = s.model_year ' ||
            ', desc1 = s.desc1 ' ||
            ', desc2 = s.desc2 ' ||
            ', desc3 = s.desc3 ' ||
            ', desc4 = s.desc4 ' ||
            ', desc5 = s.desc5 ' ||
            ', engine = s.engine ' ||
            ', drive_type = s.drive_type ' ||
            ', transmission = s.transmission ' ||
            ', mpg = s.mpg ' ||
            'from ' ||
            '(       select ls.vin, pf.manuf_name, pf.vehicle_type ' ||
                    ', pf.make_name, pf.plant_name, pf.model_year ' ||
                    ', pf.desc1, pf.desc2, pf.desc3, pf.desc4, pf.desc5 ' ||
                    ', pf.engine, pf.drive_type, pf.transmission, pf.mpg ' ||
                'from stock.unsold.lotstock ls ' ||
                'join ' ||
                '(   select' || 
                '     vin, manuf_name, vehicle_type' ||
                '    , make_name, plant_name, model_year ' ||
                '    , desc1, desc2, desc3, desc4, desc5 ' ||
                '    , engine, drive_type, transmission, mpg ' ||
                '    from table(VIN.DECODE.PARSE_AND_ENHANCE_VIN(\'' ||
                  each_row.vin || '\')) ' ||
                ') pf ' ||
                'on pf.vin = ls.vin ' ||
            ') s ' ||
            'where t.vin = s.vin;';
        res := (EXECUTE IMMEDIATE :update_stmt);
    END FOR;
    CLOSE cur;   
END;
