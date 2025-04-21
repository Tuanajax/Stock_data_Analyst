-- Active: 1733900123914@@127.0.0.1@3306@STOCK

--- DATA CLEASING:
-Check duplicate
select count(DISTINCT(symbol)) - COUNT(symbol) from Company_profile
*//

-- fomating time column in stock_prices table:
ALTER TABLE stock_prices
MODIFY COLUMN TIME DATE; 
ALTER TABLE stock_prices
ADD COLUMN ID INT AUTO_INCREMENT PRIMARY KEY;
SELECT COUNT(DISTINCT TIME,symbol,close,volume) - COUNT(*) FROM stock_prices 

CREATE INDEX ID_X
ON stock_prices(symbol(3));


with CTE AS
(SELECT *,
        ROW_NUMBER() OVER(PARTITION BY TIME,symbol,close,volume) rnm
FROM stock_prices
)
SELECT * FROM CTE
WHERE rnm > 1;
DROP PROCEDURE delete_duplicates
DELIMITER //
CREATE PROCEDURE delete_duplicates(IN table_name VARCHAR(65), IN col_name VARCHAR(65))
BEGIN
-- ADD ID_column
    ALTER TABLE table_name
    ADD COLUMN ID INT AUTO_INCREMENT PRIMARY KEY;
-- Create a temporary table to store the row numbers of duplicate rows--
    CREATE TEMPORARY TABLE duplicates AS
    SELECT ID
        FROM (
            SELECT ID, ROW_NUMBER() OVER (PARTITION BY col_name ) as row_num
            FROM table_name
            ) as subquery
            WHERE row_num > 1;

    DELETE  from table_name  
    where ID  in (SELECT ID from duplicates);  
-- Drop the temporary table.
    DROP TEMPORARY TABLE duplicates;   
END //

-- Remove duplicate in stock_prices
ALTER TABLE stock_prices
ADD COLUMN ID INT AUTO_INCREMENT PRIMARY KEY;
-- Create a temporary table to store the row numbers of duplicate rows--
CREATE TEMPORARY TABLE duplicates AS
SELECT ID
        FROM (
            SELECT ID, ROW_NUMBER() OVER (PARTITION BY TIME,symbol,close,volume  ORDER BY ID ) as row_num
            FROM stock_prices
            ) as subquery
            WHERE row_num > 1;

DELETE  from stock_prices  
where ID  in (SELECT ID from duplicates);  
-- Drop the temporary table.
DROP TEMPORARY TABLE duplicates;   



---Remove rows is
DELETE 
FROM List_company
WHERE organ_name IS NULL;


DROP PROCEDURE IF EXISTS get_bank_data;
CREATE PROCEDURE get_bank_data()
BEGIN
    DROP TEMPORARY TABLE IF EXISTS temp_bank_data;
    CREATE TEMPORARY TABLE temp_bank_data AS
    WITH CTE AS (
    SELECT
        a.symbol       'symbol',
        a.organ_name    'name_bank',
        c.bad_loan      'bad_loan',
        c.deposit        'deposit',
        ABS(c.provision)       'provision'
    FROM List_company a
    JOIN dim_table_code_industry b
    ON a.icb_code3 = b.icb_code
    JOIN Balance_sheet c
    ON a.symbol = c.symbol
    WHERE b.icb_name = 'Ngân hàng'
          AND c.year = 2024
          AND c.quarter = 3
    ORDER BY c.bad_loan DESC

  )
  SELECT
      *,
      CONCAT(ROUND(100*bad_loan/sum(bad_loan) OVER(),2),'%') 'Bad_loan_percentage',
      CONCAT(ROUND(100*deposit/sum(deposit) OVER(),2),'%') 'deposit_percentage'
  FROM CTE;
END;

call get_bank_data


DELIMITER //
--select * from stock_prices -21ms
--Create index
CREATE INDEX id_x
on stock_prices(symbol(3))
--Drop index on TABLE
ALTER TABLE stock_prices
DROP INDEX id_x;

--show index in table
SHOW INDEX FROM stock_prices;


---FILTER
SELECT MAX(TIME) AS last_date FROM stock_prices;
CREATE TEMPORARY TABLE COMPANY_HOSE AS
(
    SELECT a.symbol         'MCP',
    a.organ_name        'Name',
    c.icb_name          'Industry'    
    FROM List_company a
    JOIN Dim_exchange b
    ON a.id_exchange = b.id_exchange
    JOIN dim_table_code_industry c
    ON a.icb_code3 = c.icb_code
    where b.exchange ='HSX'
)



--- check duplicate
select COUNT(DISTINCT symbol) -COUNT(symbol)
from Company_profile;

SELECT *
FROM stock_prices
WHERE TIME = CURDATE();

select symbol,
        issue_share
from Company_profile;



--Create procedue market_overview
SELECT * from MARKET_OVERVIEW;
drop TABLE MARKET_OVERVIEW;
drop PROCEDURE MARKET_OVERVIEW;

CREATE PROCEDURE MARKET_OVERVIEW(IN MARKET VARCHAR(4))
BEGIN
    DROP TEMPORARY TABLE IF EXISTS MARKET_OVERVIEW;
    CREATE TEMPORARY TABLE MARKET_OVERVIEW AS
        SELECT  a.TIME,
                a.symbol,
                d.icb_name,
                b.issue_share,
                a.close,
                LAG(a.close, 1, a.close) OVER (PARTITION BY a.symbol ORDER BY a.TIME) as previous_close,  -- Previous day's closing price
                a.close - LAG(a.close, 1, a.close) OVER (PARTITION BY a.symbol ORDER BY a.TIME) AS CHANGE_PRICE
        FROM stock_prices a
        JOIN Company_profile b ON a.symbol = b.symbol
        JOIN List_company c ON a.symbol = c.symbol
        JOIN dim_table_code_industry d ON c.icb_code3 = d.icb_code
        JOIN Dim_exchange e ON c.id_exchange = e.id_exchange
        WHERE e.exchange = MARKET; -- Filter by the input market
END;
CALL MARKET_OVERVIEW('HSX');


--tOP 10 STOCK 
SELECT TIME,
        symbol,
        close,
        CONCAT(ROUND(CHANGE_PRICE,2),' (',ROUND(100*CHANGE_PRICE/previous_close,2),') %') AS PERCENT_CHANGE
FROM    MARKET_OVERVIEW
WHERE TIME = CURDATE()-1
ORDER BY CHANGE_PRICE/previous_close ASC
LIMIT 10;

SELECT icb_name,
       sum(issue_share*close) cap 
FROM MARKET_OVERVIEW
GROUP BY icb_name
ORDER BY cap DESC;
select * from  Balance_sheet;


DELIMITER //
CREATE PROCEDURE get_company_most_revenue()
BEGIN
    DROP TEMPORARY TABLE IF EXISTS Top_revenue;
    CREATE TEMPORARY TABLE Top_revenue AS
    SELECT 
        a.organ_name,
        SUM(c.revenue) AS total_revenue
    FROM List_company a
    JOIN PnL c ON a.symbol = c.symbol
    GROUP BY a.organ_name
    ORDER BY total_revenue DESC
    LIMIT 1;
END 
DELIMITER //
CALL get_company_most_revenue();DELIMITER //
ALTER PROCEDURE calculate_stock_returns()
BEGIN
    -- Create a temporary table to store the calculated returns
    DROP TEMPORARY TABLE IF EXISTS StockReturns;
    CREATE TEMPORARY TABLE StockReturns AS
    SELECT
        s.symbol,
        s.TIME,
        s.close,
        LAG(s.close, 1, s.close) OVER (PARTITION BY s.symbol ORDER BY s.TIME) AS previous_close,
        (s.close - LAG(s.close, 1, s.close) OVER (PARTITION BY s.symbol ORDER BY s.TIME)) / LAG(s.close, 1, s.close) OVER (PARTITION BY s.symbol ORDER BY s.TIME) AS daily_return
    FROM
        stock_prices s
    WHERE YEAR(S.TIME)=2024;
END //
DELIMITER ;
 
CALL calculate_stock_returns();

DELIMITER //
-- Create a View calculate the market cap weighted quaterly return 
CREATE VIEW quaterly_return AS
WITH CTE AS 
(
SELECT
    s.`TIME`,
    s.symbol,
    close,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, QUARTER(s.TIME),YEAR(s.TIME) ORDER BY s.TIME) AS first_day,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, QUARTER(s.TIME),YEAR(s.TIME) ORDER BY s.TIME DESC) AS last_day
FROM    Stock_returns_05_25  s
),
CTE1 AS
(
SELECT TIME,
        symbol,    
        close as close ,
        LEAD(close, 1, 0) OVER (PARTITION BY symbol, QUARTER(TIME), YEAR(TIME)  ORDER BY TIME desc) AS previous_close
FROM CTE
WHERE (first_day = 1) OR (last_day = 1)
),
CTE2 AS
(
SELECT 
        `CTE1`.TIME as  close_day,
        `CTE1`.symbol,
        `CP`.issue_share,
        `CTE1`.close as close_price,
        LN(close / previous_close) AS log_return
FROM CTE1
JOIN Company_profile CP
ON `CTE1`.symbol = CP.symbol
WHERE previous_close != 0
)
select *
from `CTE2`;
SET @LAST_DAY_2024 = (SELECT MAX(TIME) FROM stock_prices WHERE YEAR(TIME) = 2024);
-- Create a View calculate the market cap weighted monthly return 
CREATE VIEW monthly_return AS
WITH CTE AS 
(
SELECT
    s.`TIME`,
    s.symbol,
    close,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, MONTH(s.TIME),YEAR(s.TIME) ORDER BY s.TIME) AS first_day,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, MONTH(s.TIME),YEAR(s.TIME) ORDER BY s.TIME DESC) AS last_day
FROM    Stock_returns_05_25  s
),
CTE1 AS
(
SELECT TIME,
        symbol,
        close as close ,
        LEAD(close, 1, 0) OVER (PARTITION BY symbol, MONTH(TIME), YEAR(TIME) ORDER BY TIME desc) AS previous_close
FROM CTE
WHERE (first_day = 1) OR (last_day = 1)
),
CTE2 AS
(
SELECT 
        TIME as  close_day,
        `CTE1`.symbol,
        `CP`.issue_share,
        close as close_price,
        LN(close / previous_close) AS log_return
FROM CTE1
JOIN Company_profile CP
ON `CTE1`.symbol = CP.symbol
WHERE previous_close != 0
)
select *
from `CTE2`
ORDER BY symbol, close_day DESC;
-- Create a View calculate the market cap weighted yearly return 
CREATE VIEW yearly_return AS
WITH CTE AS 
(
SELECT
    s.symbol,
    s.TIME,
    close,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, YEAR(s.TIME) ORDER BY s.TIME ASC) AS first_day,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, YEAR(s.TIME) ORDER BY s.TIME DESC) AS last_day
FROM    Stock_returns_05_25  s
),
CTE1 AS
(
SELECT 
        symbol,
        TIME,
        close as close ,
        Lead(close, 1, 0) OVER (PARTITION BY symbol,  YEAR(TIME) ORDER BY TIME desc) AS previous_close
FROM CTE
WHERE (first_day = 1) OR (last_day = 1)
),
CTE2 AS
(
SELECT `CTE1`.symbol,
        `CTE1`.TIME as  close_day,
        `CP`.issue_share,
        `CTE1`.close as close_price,
        LN(close / previous_close) AS log_return
FROM CTE1
JOIN Company_profile CP
ON `CTE1`.symbol = CP.symbol
WHERE previous_close != 0
)
select * from `CTE2`
ORDER BY symbol,YEAR(close_day) DESC;

DELIMITER ;

CREATE VIEW  industry_company AS
select  b.symbol,
        a.icb_name industry
from dim_table_code_industry a, `Company_profile` b
where a.icb_code = b.industry_id_v2

select industry,
        count(symbol)
from industry_company
GROUP BY industry
HAVING count(symbol) > 10
ORDER BY count(symbol) DESC;




WITH CTE AS
(
select  YEAR(close_day) as year,
        ic.industry,
        log_return,
        rt.close_price * rt.issue_share/ (SUM(rt.close_price * rt.issue_share) over (PARTITION BY YEAR(close_day),ic.industry)) as weighted_cap
from yearly_return rt
JOIN industry_company ic
on ic.symbol = rt.symbol
)
select    1,
        year,
        industry,
        100*(EXP(AVG(log_return))-1) as log_total_return,
        100*(EXP(sum(log_return*weighted_cap))-1) as log_total_return_weighted,
        100*STDDEV(log_return)   as volatility,
        100*STDDEV(log_return*weighted_cap)   as wei_volatility
from CTE
where industry = 'Ngân hàng'
GROUP BY year, industry;



WITH CTE AS
(
select   close_day year,
        ic.industry,
        log_return,
        rt.close_price * rt.issue_share/ (SUM(rt.close_price * rt.issue_share) over (PARTITION BY YEAR(close_day),MONTH(close_day),ic.industry)) as weighted_cap
from monthly_return rt
JOIN industry_company ic
on ic.symbol = rt.symbol
)
select    2,
        MONTH(YEAR),
        industry,
        100*(EXP(AVG(log_return))-1) as log_total_return,
        100*(EXP(sum(log_return*weighted_cap))-1) as log_total_return_weighted,
        100*STDDEV(log_return)   as volatility,
        100*STDDEV(log_return*weighted_cap)   as wei_volatility
from CTE
where industry = 'Ngân hàng'
GROUP BY YEAR, industry
ORDER BY log_total_return DESC

select 
        YEAR(close_day) as year,
        ic.industry,
               
        100*(exp(AVG(log_return))-1) as log_total_return
from yearly_return rt
JOIN industry_company ic
on rt.symbol = ic.symbol
where industry = 'Ngân hàng'
GROUP BY YEAR(close_day), ic.industry
ORDER BY log_total_return DESC;

select 
        ic.industry, 
        100*(exp(AVG(log_return))-1) as log_total_return,
        AVG(log_return)/STDDEV(log_return) as sharpe_ratio
from yearly_return rt
JOIN industry_company ic
on rt.symbol = ic.symbol
GROUP BY  ic.industry
ORDER BY log_total_return DESC, sharpe_ratio ASC;

with CTE AS
(
    SELECT ic.industry,
        year(rt.close_day) AS year,
        log_return,
        sum(issue_share * close_price) over (PARTITION BY YEAR(close_day), ic.industry) as total_cap,
        issue_share * close_price/sum(issue_share * close_price) over (PARTITION BY YEAR(close_day), ic.industry) as weighted_cap
from yearly_return rt
JOIN industry_company ic
on rt.symbol = ic.symbol
)
select industry,
        COUNT(industry) as num_company,
        year,
        avg(total_cap)  as avg_cap,
        100*(exp(AVG(log_return))-1) as log_total_return,
        100*(exp(sum(log_return*weighted_cap))-1) as log_total_return_weighted,
        100*STDDEV(log_return) as volatility,
        100*STDDEV(log_return*weighted_cap) as wei_volatility
from CTE
GROUP BY industry, year
HAVING year = 2012
ORDER BY avg_cap DESC,
        log_total_return DESC;



WITH CTE AS
(
    SELECT ic.industry,
        YEAR(close_day) as year,
        QUARTER(close_day) as quarter,
        log_return,
        sum(issue_share * close_price) over (PARTITION BY YEAR(close_day),QUARTER(rt.close_day),ic.industry) as total_cap,
        issue_share*close_day/sum(issue_share*close_day) OVER (PARTITION BY YEAR(rt.close_day), QUARTER(rt.close_day), ic.industry) as weighted_cap
    from  quaterly_return rt
    JOIN industry_company ic
    on rt.symbol = ic.symbol
),
CTE1 AS
(
select  industry,
        year,
        quarter ,
        exp(sum(log_return*weighted_cap)) -1 as log_return_total,
        sum(log_return*weighted_cap) as log_return_weighted
from CTE
GROUP BY industry,year, quarter
ORDER BY quarter, AVG(total_cap) DESC
)
select year,quarter,industry,
        AVG(log_return_total) as log_return_total,
        exp(AVG(log_return_weighted))-1
from `CTE1`
WHERE industry = 'Ngân hàng'
GROUP BY year,quarter,industry
ORDER BY year,quarter;

WITH CTE AS
(
    SELECT ic.industry,
        YEAR(close_day) as year,
        MONTH(close_day) as month,
        log_return,
        sum(issue_share * close_price) over (PARTITION BY YEAR(close_day), MONTH(rt.close_day), ic.industry) as total_cap,
        issue_share*close_day/sum(issue_share*close_day) OVER (PARTITION BY YEAR(rt.close_day), MONTH(rt.close_day), ic.industry) as weighted_cap
    from  monthly_return rt
    JOIN industry_company ic
    on rt.symbol = ic.symbol
),
CTE1 AS
(
select  industry,
        year,
        month,
        exp(sum(log_return*weighted_cap)) -1 as log_return_total,
        sum(log_return*weighted_cap) as log_return_weighted
from CTE
GROUP BY industry,year, month
ORDER BY AVG(total_cap) DESC
),
CTE2 AS
(
select industry,month,
        AVG(log_return_total) as log_return_total,
        exp(AVG(log_return_weighted))-1
from `CTE1`
GROUP BY industry, month
)
select industry,
      100*(EXP(12*AVG(log_return_total))-1),
      100*AVG(log_return_total) as log_return_total,
      100*(EXP(AVG(log_return_total))-1)
from `CTE2`
GROUP BY industry;


-- Create a View calculate the market cap weighted weekly return 
CREATE VIEW weekly_return AS
WITH CTE AS 
(
SELECT
    s.`TIME`,
    s.symbol,
    close,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, WEEK(s.TIME),YEAR(s.TIME) ORDER BY s.TIME) AS first_day,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, WEEK(s.TIME),YEAR(s.TIME) ORDER BY s.TIME DESC) AS last_day
FROM    Stock_returns_05_25  s
),
CTE1 AS
(
SELECT TIME,
        symbol,
        close as close ,
        LEAD(close, 1, 0) OVER (PARTITION BY symbol, WEEK(TIME), YEAR(TIME) ORDER BY TIME desc) AS previous_close
FROM CTE
WHERE (first_day = 1) OR (last_day = 1)
),
CTE2 AS
(
SELECT 
        TIME as  close_day,
        `CTE1`.symbol,
        `CP`.issue_share,
        close as close_price,
        LN(close / previous_close) AS log_return
FROM CTE1
JOIN Company_profile CP
ON `CTE1`.symbol = CP.symbol
WHERE previous_close != 0
)
select *
from `CTE2`
ORDER BY symbol, close_day DESC;



alter view weekly_return_industry as
WITH CTE AS
(
    SELECT ic.industry,
        rt.symbol,
        YEAR(close_day) as year,
        WEEK(close_day) as week,
        log_return,
        sum(issue_share * close_price) over (PARTITION BY YEAR(close_day), WEEK(rt.close_day), ic.industry) as total_cap,
        issue_share*close_day/sum(issue_share*close_day) OVER (PARTITION BY YEAR(rt.close_day), WEEK(rt.close_day), ic.industry) as weighted_cap
    from  weekly_return rt
    JOIN industry_company ic
    on rt.symbol = ic.symbol
),
CTE1 AS
(
select year,industry,symbol,
        100*(EXP(AVG(log_return))-1) avg_return
from CTE
GROUP BY year, industry,symbol
ORDER BY AVG(total_cap) DESC
)
SELECT * from CTE1;





CREATE view weekly_return_industry 
CTE1 AS
(
select  industry,
        year,
        week,
        AVG(log_return) as avg_log_return
from CTE
GROUP BY industry,year, week
ORDER BY AVG(total_cap) DESC
),
CTE2 AS
(
SELECT industry,
        year,
        100*(EXP(52*AVG(avg_log_return))-1) as anualized_return
from CTE1
GROUP BY industry,
        year
)
select industry,
      AVG(anualized_return)
from `CTE2`
GROUP BY industry;
CREATE TABLE IF NOT EXISTS financial_reports (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ma VARCHAR(10),
        ten_cong_ty VARCHAR(255),
        loai_bao_cao VARCHAR(255),
        quy_4_2024 VARCHAR(50),
        quy_4_2023 VARCHAR(50),
        thay_doi VARCHAR(50),
        thoi_gian_gui VARCHAR(50),
        tai_ve TEXT,
        pdf_path VARCHAR(255)
    );
DROP TABLE financial_reports;
SELECT *
from financial_reports;


create TABLE daily_return_15_20 AS
select  rr.symbol,
        rr.TIME,
        rr.close,
        rr.daily_return
from  stock_returns_05_25 rr
WHERE YEAR(TIME) BETWEEN 2015 AND 2020
    AND length(symbol) < 4;

DROP TABLE daily_return;
ALTER VIEW liquidity AS
SELECT  symbol,
        AVG(volume) avg_volume,
        STDDEV(volume) std_volume,
        STDDEV(volume)/AVG(volume) as cv_volume
from stock_prices
wHERE TIME BETWEEN '2015-01-01' AND '2024-12-31'
GROUP BY symbol
ORDER BY AVG(volume) DESC, cv_volume ASC
LIMIT 100;

CREATE VIEW Volume  AS
SELECT symbol,
        volume
from stock_prices
where TIME BETWEEN '2012-01-01' AND '2023-12-31';


select 
        CONCAT('ACB: ',STDDEV(volume)/AVG(volume)) as cv_volume
from stock_prices
WHERE symbol = 'ACB';

select * from `Stock_returns_05_25`;

ALTER VIEW view_finance_ratio AS 
SELECT CP           'Symbol',
        concat(`Kỳ`,'/',`Năm`)      'Quarter',
        `P/E`        'P/E',
        `P/B`       'P/B',    
        `EPS (VND)`     'EPS'
from finance_ratio;

SELECT * from view_finance_ratio;

ALTER VIEW Stock_returns_12_23 AS
select  Symbol,
        TIME,
        daily_return
from `Stock_returns_05_25`
where TIME BETWEEN '2012-01-01' AND '2023-12-31';

select * from `Company_profile`;

ALTER VIEW market_cap_weighted_return AS
SELECT  
        CP.symbol,
        CP.industry,
        100*issue_share * close/ SUM(issue_share * close) OVER () AS market_cap_percent
FROM `Company_profile` CP
JOIN stock_prices sp
ON CP.symbol = sp.symbol
WHERE `TIME` ='2024-12-31'

SELECT * from  market_cap_weighted_return
ORDER BY market_cap_percent DESC;

select * from `Cash_Flow`
where ticker = 'VIC';
ALTER View  FCF AS
SELECT  CP.industry,
        CF.ticker symbol,
        CF.`yearReport` year,
        CF.`Net cash inflows/outflows from operating activities` OCF,
        (CF.`Purchase of fixed assets` + CF.`Proceeds from disposal of fixed assets`) as CAPEX,
        (CF.`Net cash inflows/outflows from operating activities` - (CF.`Purchase of fixed assets` + CF.`Proceeds from disposal of fixed assets`))   FCF
FROM `Cash_Flow` CF
JOIN Company_profile CP
ON CF.ticker = CP.symbol

select * from FCF
