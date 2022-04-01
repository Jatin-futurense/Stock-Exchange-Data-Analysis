Creating SEDA_database
--------------------------

create database SEDA_database;

use SEDA_database;

Creating tables
--------------------------------

create table STOCK_PRICES 
(
Trading_date date,
Symbol varchar(21),
Open double,
Close double,
Low double,
High double,
Volume int
);


create table STOCK_COMPANIES
(
Symbol varchar(21),
Company_name varchar(21),
Sector varchar(21),
Sub_industry varchar(21),
Headquarter varchar(21)
);


Loading data in tables
-------------------------

SET GLOBAL local_infile=1;

load data local infile '/home/saif/Downloads/NYSE/StockPrices.csv' into table STOCK_PRICES fields terminated by ','
 lines terminated by '\n';
 
load data local infile '/home/saif/Downloads/NYSE/Stockcompanies.csv' into table STOCK_COMPANIES fields terminated
by ',' lines terminated by '\n';


Now import the data from mysql using sqoop to hive
---------------------------------------------------

sqoop import --connect jdbc:mysql://localhost/SEDA_database --username root --password Welcome@123 \
--delete-target-dir --table STOCK_COMPANIES --hive-import --hive-database stockanalysis -m 1

sqoop import --connect jdbc:mysql://localhost/SEDA_database --username root --password Welcome@123 \
--delete-target-dir --table STOCK_PRICES -–hive-import --hive-database stockanalysis --m 1 

Create a new hive table by joining the above 2 hive tables 
----------------------------------------------------------- 

create table stock_data as select trading_year,trading_month, sc.symbol, company_name,headquarter state,
sector, sub_industry, open, close, low, high, volume from stock_companies sc,(select symbol, year(trading_date) trading_year, 
month(trading_date) trading_month,round(avg(open),2) open, round(avg(close),2) close, round(avg(low),2) low,round(avg(high),2) high, 
round(avg(volume),2) volumefrom stock_prices group by symbol, month(trading_date),year(trading_date)) sp where sc.symbol=sp.symbol;

1)Find the top five companies that are good for investment Step
================================================================= 
step 1: Create a temp table with required data for analysis
----------------------------------------------------------------

create table company_horizon as select company_name, min(trading_year) min, max(trading_year) max,
min(trading_month) min_month, max(trading_month) max_month from stock_data group by company_name;

Step 2: Alalyze based on the data in temp table created to identify the growth of a company.
------------------------------------------------------------------------------------------------------
Find the top five companies that are good for investment
-----------------------------------------------------------

select stock_start.company_name,((close-open)/open)*100 growth_percent from 
(select chv.company_name, open from stock_data sd, company_horizon chv where sd.trading_year = chv.min 
and sd.trading_month = chv.min_month and sd.company_name = chv.company_name) stock_start,           
(select chv.company_name,close from stock_data sd, company_horizon chv where sd.trading_year = chv.max 
and sd.trading_month = chv.max_month and sd.company_name = chv.company_name) stock_end 
where stock_start.company_name = stock_end.company_name sort by growth_percent desc limit 5;




2)Show the best-growing industry by each state, having at least two or more industries mapped
--------------------------------------------------------------------------------------------------
step 1:
---------

create table company_growth as select state, sub_industry, stock_start.company_name, 
((stock_end.close-stock_start.open)/stock_start.open)*100 growth_percent from (select chv.company_name,open            
from stock_data sd, company_horizon chv where sd.trading_year=chv.min and                        
sd.trading_month=chv.min_month and sd.company_name=chv.company_name)stock_start,         
(select chv.company_name, close from stock_data sd, company_horizon chv where sd.trading_year=chv.max and                        
sd.trading_month=chv.max_month and sd.company_name=chv.company_name)stock_end,          
(select company_name, state, sub_industry from stock_data            
group by company_name,state,sub_industry)sd where (stock_end.close-stock_start.open)>0 and           
stock_start.company_name=stock_end.company_name and sd.company_name=stock_start.company_name;



step 2:
----------

create table industry_growth as select state,sub_industry, avg(growth_percent) ind_growth 
from company_growth group by state, sub_industry ;

step 3:
-----------

select ig.state, sub_industry, ind_growth from industry_growth ig, (select state,max(ind_growth) max_growth          
from industry_growth group by state) inn_ig where inn_ig.state = ig.state and ig.ind_growth = inn_ig.max_growth;

3)For each sector find the following.
-------------------------------------
-Worst year<br>
-Best year<br>
-Stable year<br>

step 1:
--------

create table sector_growth as select open.sector, open.trading_year,(close-open) growth from (select sector,trading_year,avg(open) open
from stock_data where trading_month = 1 group by sector,trading_year) open, (select sector,trading_year,avg(close) close
from stock_data where trading_month=12 group by sector,trading_year) close where open.sector = close.sector and       
open.trading_year = close.trading_year;

step 2:
--------
For the worst trading year by sector
----------------------------------------
select x.sector,x.trading_year,x.growth from sector_growth x, (select sector,min(growth) growth from sector_growth           
group by sector) y where x.sector=y.sector and x.growth=y.growth;


For the best trading year by sector:
----------------------------------------

select x.sector,x.trading_year,x.growth from sector_growth x,(select sector,max(growth) growth           
from sector_growth group by sector) ywhere x.sector=y.sector and x.growth=y.growth;

For the stable year by sector
-------------------------------------

select x.sector,x.trading_year,round(x.growth,0) from sector_growth x,(select sector,round(avg(growth),0) growth           
from sector_growth group by sector) y where x.sector=y.sector and x.growth=y.growth;
