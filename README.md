# Stock-Exchange-Data-Analysis

<h3>DESCRIPTION</h3>
<h4>Objective:</h4> To use hive features for data engineering or analysis and sharing the actionable insights
<h4>Problem Statement:</h4>
NewYork stock exchange data of seven years, between 2010 to 2016, is captured for 500+ listed companies. The data set comprises of intra-day prices and volume traded for each listed company. The data serves both for machine learning and exploratory analysis projects, to automate the trading process and to predict the next trading-day winners or losers.. The scope of this project is limited to exploratory data analysis.
<h4>Domain:</h4> BFSI
<h4>Analysis to be done:</h4> Exploratory analysis to understand how MoM or YoY companies from different sectors or industries and states have progressed in a period of 7 years
<h4>Content:</h4> This data set contains prices.csv and securities.csv files having the following features:
<h5>Prices.csv:</h5>
1.Date: Trading date<br>
2.Symbol: Ticker code or listed company code on NY stock exchange<br>
3.Open: Intra-day opening price for each listed company<br>
4.Close: Intra-day closing price for each listed company<br>
5.Low: Intra-day lowest price for each listed company<br>
6.High: Intra-day highest price for each listed company<br>
7.Volume: Number of shares traded per day per company<br>
<h5>Securities.csv:</h5>
1.Ticker_Symbol: Country to which the customer belongs<br>
2.Security: Legal name of the listed company<br>
3.Sector: Business vertical of the listed company<br>
4.Sub_Industry: Business domain of the listed company within a Sector.<br>
5.Headquarter: Headquarters address<br>
<h4>Steps to perform:</h4>
     1) Create a data pipeline using sqoop to pull the data from the table below from MYSQL server into Hive.<br>
i. Stock_prices<br>
ii. Stock_companies<br>
Check the TABLE description: 

<h5>TABLE: STOCK_PRICES</h5>

<h5>TABLE: STOCK_COMPANIES</h5>

2) Create a new hive table with the following fields by joining the above two hive tables.<br>
Please use appropriate Hive built-in functions for columns (a,b,e and h to l).<br>
<ul>
<li>Trading_year: Should contain YYYY for each record</li>
<li>Trading_month: Should contain MM or MMM for each record</li>
<li>Symbol: Ticker code</li>
<li>CompanyName: Legal name of the listed company</li>
<li>State: State to be extracted from headquarters value.</li>
<li>Sector: Business vertical of the listed company</li>
<li>Sub_Industry: Business domain of the listed company within a sector</li>
<li>Open: Average of intra-day opening price by month and year for each listed company</li>
<li>Close: Average of intra-day closing price by month and year for each listed company</li>
<li>Low: Average of intra-day lowest price by month and year for each listed company</li>
<li>High: Average of intra-day highest price by month and year for each listed company</li>
<li>Volume: Average of number of shares traded by month and year for each listed company</li>
</ul>
<h4>DATA ANALYSIS USING HIVE</h4>

 3) Find the top five companies that are good for investment<br>
 4) Show the best-growing industry by each state, having at least two or more industries mapped.<br>
 5) For each sector find the following.<br>
    a.) Worst year<br>
    b.) Best year<br>
    c.) Stable year<br>
