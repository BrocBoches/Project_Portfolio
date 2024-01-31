USE cryptopunk;
-- 1.	How many sales occurred during this time period? 
SELECT COUNT(*) FROM pricedata;
-- 2.	Return the top 5 most expensive transactions (by USD price) for this data set. Return the name, ETH price, and USD price, as well as the date.
SELECT name, eth_price, usd_price, event_date, RANK() OVER(ORDER BY usd_price DESC) 
FROM pricedata
LIMIT 5;
-- 3.	Return a table with a row for each transaction with an event column, a USD price column, and a moving average of USD price that averages the last 50 transactions.
SELECT event_date, usd_price, 
	ROUND(AVG(usd_price) OVER(ORDER BY event_date DESC ROWS BETWEEN 50 PRECEDING AND CURRENT ROW),2) AS moving_average 
FROM pricedata;
-- 4.	Return all the NFT names and their average sale price in USD. Sort descending. Name the average column as average_price.
SELECT DISTINCT name, AVG(usd_price) OVER(PARTITION BY name ORDER BY name DESC) AS average_price 
FROM pricedata; 
-- 5.	Return each day of the week and the number of sales that occurred on that day of the week, as well as the average price in ETH. Order by the count of transactions in ascending order.
SELECT DISTINCT dayofweek(event_date), 
	COUNT(dayofweek(event_date)) OVER(PARTITION BY dayofweek(event_date)) AS count_transactions, 
	AVG(eth_price) OVER (PARTITION BY dayofweek(event_date)) AS AVG_ETH_price 
FROM pricedata
ORDER BY count_transactions; 
-- 6.	Construct a column that describes each sale and is called summary. 
-- The sentence should include who sold the NFT name, who bought the NFT, who sold the NFT, the date, and what price it was sold for in USD rounded to the nearest thousandth.
-- Here’s an example summary:
-- “CryptoPunk #1139 was sold for $194000 to 0x91338ccfb8c0adb7756034a82008531d7713009d from 0x1593110441ab4c5f2c133f21b0743b2b43e297cb on 2022-01-14”
SELECT CONCAT(name, " was sold for $", ROUND(usd_price, -3), " to ", buyer_address, " from ", seller_address, " on ", event_date) AS Summary 
FROM pricedata;
-- 7.	Create a view called “1919_purchases” and contains any sales where “0x1919db36ca2fa2e15f9000fd9cdc2edcf863e685” was the buyer.
CREATE VIEW 1919_purchases AS
SELECT * FROM pricedata
WHERE buyer_address = "0x1919db36ca2fa2e15f9000fd9cdc2edcf863e685";
SELECT * FROM 1919_purchases;
-- 8.	Create a histogram of ETH price ranges. Round to the nearest hundred value. 
SELECT ROUND(Eth_price,-2) AS Bucket,
COUNT(*) AS COUNT,
RPAD('', COUNT(*), '-') AS Bar
FROM pricedata
GROUP BY Bucket
ORDER BY Bucket;
-- 9.	Return a unioned query that contains the highest price each NFT was bought for and a new column called status saying “highest” 
	--  with a query that has the lowest price each NFT was bought for and the status column saying “lowest”. 
	--  The table should have a name column, a price column called price, and a status column. Order the result set by the name of the NFT, and the status, in ascending order. 
WITH cte_max AS (SELECT name, USD_Price, 
MAX(USD_price) OVER(PARTITION BY name) AS MAX_Price 
FROM pricedata),
cte_min AS (SELECT name, USD_Price,
MIN(USD_Price) OVER(PARTITION BY name) AS MIN_Price 
FROM pricedata),
cte_max1 AS (SELECT *, CASE
	WHEN MAX_price = USD_Price THEN "Highest"
    ELSE ''
END AS Status
FROM cte_max),
cte_min1 AS (SELECT *, CASE
	WHEN MIN_price = USD_price THEN "Lowest"
    ELSE ''
END AS Status
FROM cte_min) 
SELECT name, USD_price, Status FROM cte_max1 WHERE Status != '' UNION
SELECT name, USD_price, Status FROM cte_min1 WHERE Status != '' 
ORDER BY name, Status;
-- 10.	What NFT sold the most each month / year combination? Also, what was the name and the price in USD? Order in chronological format. 
SELECT DISTINCT name, MONTH(event_date) AS 'Month', YEAR(event_date) AS 'Year' , 
	COUNT(*) OVER(partition BY MONTH(event_date), year(event_date), name) AS Total_sales, 
	ROUND(AVG(USD_price) OVER(partition BY name),2) AS avg_price 
FROM pricedata
group by name, month(event_date), year(event_date), usd_price
ORDER BY Total_sales DESC;
-- 11.	Return the total volume (sum of all sales), round to the nearest hundred on a monthly basis (month/year).
SELECT MONTH(event_date) AS Month, YEAR(event_date) AS Year, ROUND(SUM(usd_price),-2) AS Total_Volume FROM pricedata
GROUP BY YEAR(event_date), MONTH(event_date)
ORDER BY YEAR(event_date), MONTH(event_date);
-- 12.	Count how many transactions the wallet "0x1919db36ca2fa2e15f9000fd9cdc2edcf863e685"had over this time period.
SELECT COUNT(*) AS Trasactions FROM pricedata
WHERE buyer_address = '0x1919db36ca2fa2e15f9000fd9cdc2edcf863e685' OR seller_address = '0x1919db36ca2fa2e15f9000fd9cdc2edcf863e685';
-- 13.	Create an “estimated average value calculator” that has a representative price of the collection every day based off of these criteria:
--  - Exclude all daily outlier sales where the purchase price is below 10% of the daily average price
--  - Take the daily average of remaining transactions
-- a) First create a query that will be used as a subquery. Select the event date, the USD price, and the average USD price for each day using a window function. Save it as a temporary table.
CREATE TEMPORARY TABLE Price_data 
SELECT DISTINCT event_date, USD_Price, AVG(USD_price) OVER(PARTITION BY event_date) AS Avg_price_day FROM pricedata;
--  b) Use the table you created in Part A to filter out rows where the USD prices is below 10% of the daily average and return a new estimated value which is just the daily average of the filtered data
SELECT *, AVG(USD_price) OVER(PARTITION BY event_date) AS New_avg FROM Price_data 
WHERE NOT USD_price < .9*AVG_price_day;
-- 14.	Give a complete list ordered by wallet profitability (whether people have made or lost money)
CREATE TEMPORARY TABLE Buyer
	SELECT DISTINCT Buyer_address, SUM(usd_price) OVER(PARTITION BY buyer_address) AS Buyer  FROM pricedata
	WHERE Seller_address != ''  AND buyer_address != '';

CREATE TEMPORARY TABLE Seller
	SELECT DISTINCT Seller_address, SUM(usd_price) OVER(PARTITION BY seller_address) AS Seller FROM pricedata
	WHERE Seller_address != ''  AND buyer_address != ''
	ORDER BY seller_address;

CREATE TEMPORARY TABLE Profit
	SELECT * FROM Buyer
	JOIN Seller
	ON Buyer.buyer_address = Seller.Seller_address;

SELECT buyer_address AS Address, Buyer, Seller, ROUND(Seller-Buyer,2) AS Profit FROM Profit
ORDER BY Profit DESC;
