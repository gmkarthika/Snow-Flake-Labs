--Create database ZENAS_ATHLEISURE_DB
CREATE DATABASE ZENAS_ATHLEISURE_DB;

--create schema PRODUCTS
create schema ZENAS_ATHLEISURE_DB.PRODUCTS;

--create internal stage and load the Sweatsuit Files Into It
--Run a list command
list @zenas_athleisure_db.products.product_metadata;

select $1 from @zenas_athleisure_db.products.product_metadata/product_coordination_suggestions.txt;
select $1 from @zenas_athleisure_db.products.product_metadata/sweatsuit_sizes.txt;
select $1 from @zenas_athleisure_db.products.product_metadata/swt_product_line.txt;

--Create an Exploratory File Format
create or replace file format zmd_file_format_1
RECORD_DELIMITER  = ';'
TRIM_SPACE = TRUE;

--Use the Exploratory File Format in a Query
select $1 from @zenas_athleisure_db.products.product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_1) ;

--Create an Exploratory File Format 2
create or replace file format zmd_file_format_2
FIELD_DELIMITER  = '|'
RECORD_DELIMITER  = ';'
TRIM_SPACE = TRUE;

--Use the Exploratory File Format in a Query
select $1,$2,$3 from @zenas_athleisure_db.products.product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_2) ;

--Create an Exploratory File Format 3
create file format zmd_file_format_3
FIELD_DELIMITER  = '='
record_delimiter = '^';

--Use the Exploratory File Format in a Query
select $1,$2 from @zenas_athleisure_db.products.product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3) ;

--Use the Exploratory File Format in a Query
--can use 'chr(13)||char(10)' instead of '\r\n'
select REPLACE($1,chr(13)||char(10)) as sizes_available 
from @zenas_athleisure_db.products.product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1)
where sizes_available <> '';

--Use the Exploratory File Format in a Query
select REPLACE($1,chr(13)||char(10)),$2,$3 from @zenas_athleisure_db.products.product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2
) ;

--create view for sweatsuit_sizes.txt
create view zenas_athleisure_db.products.sweatsuit_sizes as 
(select REPLACE($1,chr(13)||char(10)) as sizes_available 
from @zenas_athleisure_db.products.product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1)
where sizes_available <> '');

----create view for swt_product_line.txt
create or replace view zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE as
(select REPLACE($1,chr(13)||char(10)) as product_code,$2 as headband_description,$3 as wristband_description from @zenas_athleisure_db.products.product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2
));

----create view for product_coordination_suggestions.txt
create view zenas_athleisure_db.products.SWEATBAND_COORDINATION as
(select $1 as PRODUCT_CODE,$2 as HAS_MATCHING_SWEATSUIT from @zenas_athleisure_db.products.product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3)) ;

--select sweatband_coordination to view the table
select product_code, has_matching_sweatsuit
from zenas_athleisure_db.products.sweatband_coordination;

--select sweatband_product_line to view the table
select product_code, headband_description, wristband_description
from zenas_athleisure_db.products.sweatband_product_line;

--select sweatsuit_sizes to view the table
select sizes_available
from zenas_athleisure_db.products.sweatsuit_sizes;

--Run a List Command On the SWEATSUITS Stage
list  @ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS;

select $1 
from @ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS.purple_sweatsuit.png;

select metadata$filename, metadata$file_row_number
from @sweatsuits/purple_sweatsuit.png;

--query to return file name and row count
select metadata$filename, count(*)
from @sweatsuits
group by metadata$filename;

--Query the Directory Table of a Stage
select *
from directory(@sweatsuits);

--Start By Checking Whether Functions will Work on Directory Tables
select REPLACE(relative_path, '_', ' ') as no_underscores_filename
, REPLACE(no_underscores_filename, '.png') as just_words_filename
, INITCAP(just_words_filename) as product_name
from directory(@sweatsuits);

--Nest 3 Functions into 1 Statement
select INITCAP(REPLACE(REPLACE(relative_path, '_', ' '),'.png')) as just_words_filename
from directory(@sweatsuits);

--create an internal table for some sweatsuit info
create or replace table zenas_athleisure_db.products.sweatsuits (
	color_or_style varchar(25),
	file_name varchar(50),
	price number(5,2)
);

--fill the new table with some data
insert into  zenas_athleisure_db.products.sweatsuits 
          (color_or_style, file_name, price)
values
 ('Burgundy', 'burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'forest_green_sweatsuit.png',64)
,('Navy Blue', 'navy_blue_sweatsuit.png',65)
,('Orange', 'orange_sweatsuit.png',65)
,('Pink', 'pink_sweatsuit.png',63)
,('Purple', 'purple_sweatsuit.png',64)
,('Red', 'red_sweatsuit.png',68)
,('Royal Blue',	'royal_blue_sweatsuit.png',65)
,('Yellow', 'yellow_sweatsuit.png',67);

--join directory table with regular table
create view PRODUCT_LIST as
(select INITCAP(REPLACE(REPLACE(relative_path, '_', ' '),'.png')) as product_name
,file_name, color_or_style,price,file_url
from directory(@sweatsuits) d
join sweatsuits s
on d.relative_path = s.file_name)
;

--create a catalog view by cross join
create view CATALOG  as(
select * from 
PRODUCT_LIST p
cross join 
SWEATSUIT_SIZES
);
-- Add a table to map the sweatsuits to the sweat band sets
create table zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style varchar(25)
,upsell_product_code varchar(10)
);

--populate the upsell table
insert into zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style
,upsell_product_code 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');


-- Zena needs a single view she can query for her website prototype
create view catalog_for_website as 
select color_or_style
,price
,file_name
, get_presigned_url(@sweatsuits, file_name, 3600) as file_url
,size_list
,coalesce('Consider: ' ||  headband_description || ' & ' || wristband_description, 'Consider: White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, file_name
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, file_name
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code;

