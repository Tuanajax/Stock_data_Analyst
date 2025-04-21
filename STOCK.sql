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

SELECT * FROM StockReturns;






