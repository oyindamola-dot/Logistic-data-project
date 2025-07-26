USE Logistics;

/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [Timestamp]
      ,[Asset_ID]
      ,[Latitude]
      ,[Longitude]
      ,[Inventory_Level]
      ,[Shipment_Status]
      ,[Temperature]
      ,[Humidity]
      ,[Traffic_Status]
      ,[Waiting_Time]
      ,[User_Transaction_Amount]
      ,[User_Purchase_Frequency]
      ,[Logistics_Delay_Reason]
      ,[Asset_Utilization]
      ,[Demand_Forecast]
      ,[Logistics_Delay]
  FROM [Logistics].[dbo].[Logistics]


-- Understanding the data structure: Checking for NULL values

SELECT 
  COUNT(*) AS total_records,
  SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp,
  SUM(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) AS null_asset_id,
  SUM(CASE WHEN logistics_delay IS NULL THEN 1 ELSE 0 END) AS null_logistic_delay
FROM Logistics;


/*	Identify and Quantify Delay Patterns
	Delay distribution */

SELECT logistics_delay, COUNT(*) AS count_of_delay
FROM Logistics
GROUP BY logistics_delay
ORDER BY count_of_delay DESC;


-- Delay reason breakdown

SELECT logistics_delay_reason, COUNT(*) AS No_Of_Delay
FROM Logistics
WHERE logistics_delay = 1
GROUP BY logistics_delay_reason
ORDER BY NO_Of_Delay DESC;

-- Delay Vs Temperature

SELECT 
  ROUND(temperature, 0) AS temp_band, -- Rounded temperature groupings

  SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) AS delayed, -- Number of delayed entries

  COUNT(*) AS total, -- Total number of records in each temp_band

  -- Compute delay rate and format to 2 significant figures using STR()
  STR(
    ROUND(100.0 * SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) / COUNT(*), 10),6,2 ) AS delay_rate

FROM Logistics
GROUP BY ROUND(temperature, 0)
ORDER BY temp_band;



-- Delay vs Humidity

SELECT 
  ROUND(humidity, 0) AS humidity_band, -- Rounded humidity values grouped into whole-number bands

  SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) AS delayed, -- Number of logistics entries delayed in each humidity band

  COUNT(*) AS total, -- Total number of entries in each humidity band

  -- Calculate delay rate as a percentage and format with 2 decimal digits using STR()
  STR(
    ROUND(
      100.0 * SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) / COUNT(*),
      10 -- Precision boost before formatting
    ),
    6, -- Total string length including decimal
    2  -- Decimal places for approximate 2 significant figures
  ) AS delay_rate -- Formatted delay rate with roughly 2 significant digits

FROM Logistics -- Source table containing humidity and logistics delay data

GROUP BY ROUND(humidity, 0) -- Group by humidity bands

ORDER BY humidity_band; -- Sorted by humidity for easy readability




-- Delay Vs Traffic

SELECT 
  traffic_status, -- The traffic condition during each transaction
  
  COUNT(*) AS transaction_count, -- Total number of transactions under each traffic status
  
  SUM(CAST(User_Transaction_Amount AS DECIMAL(18,1))) AS transaction_amount, -- Total monetary value of transactions (converted from text to decimal)
  
  SUM(
    CASE 
      WHEN logistics_delay = 1 THEN CAST(User_Transaction_Amount AS DECIMAL(18,1)) 
      ELSE 0 
    END
  ) AS delayed_amount, -- Sum of transaction values that experienced a logistics delay
  
  CAST(
    ROUND(
      100.0 * SUM(CASE WHEN logistics_delay = 1 THEN CAST(User_Transaction_Amount AS DECIMAL(18,2)) ELSE 0 END) 
      / SUM(CAST(User_Transaction_Amount AS DECIMAL(18,2))), 
    2) 
    AS DECIMAL(6,2)
  ) AS value_delay_rate -- Percentage of transaction value affected by delay, rounded to 2 decimal places

INTO DBT -- creating a new table for this as Delay by Traffic (DBT)

FROM Logistics  -- Source table containing logistics data

GROUP BY traffic_status -- Group data by traffic conditions

ORDER BY value_delay_rate DESC; -- Show traffic statuses with the highest delay impact first


SELECT * FROM DBT;



CREATE TABLE DelayImpactByHour (
  hour_of_day INT,
  delayed_transactions INT,
  total_transactions INT,
  delay_rate NVARCHAR(10),
  total_value NVARCHAR(20),
  avg_transaction_value NVARCHAR(20)
);

-- IMPACT OF DELAY PER HOUR

WITH HourlyStats AS (
  SELECT 
    DATEPART(HOUR, timestamp) AS hour_of_day,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) AS delayed_transactions,
    SUM(TRY_CAST(User_Transaction_Amount AS FLOAT)) AS total_value,
    AVG(TRY_CAST(User_Transaction_Amount AS FLOAT)) AS avg_value
  FROM [dbo].[Logistics]
  WHERE ISNUMERIC(User_Transaction_Amount) = 1
  GROUP BY DATEPART(HOUR, timestamp)
),
DelayRateCalc AS (
  SELECT 
    hour_of_day,
    delayed_transactions,
    total_transactions,
    total_value,
    avg_value,
    CAST(delayed_transactions AS FLOAT) / total_transactions AS delay_rate
  FROM HourlyStats
)
INSERT INTO DelayImpactByHour (
  hour_of_day,
  delayed_transactions,
  total_transactions,
  delay_rate,
  total_value,
  avg_transaction_value
)
SELECT 
  hour_of_day,
  delayed_transactions,
  total_transactions,
  FORMAT(delay_rate * 100, 'N2') + '%' AS delay_rate,
  FORMAT(total_value, 'N2') AS total_value,
  FORMAT(avg_value, 'N2') AS avg_transaction_value
FROM DelayRateCalc;

SELECT * FROM DelayImpactByHour

ORDER BY delay_rate DESC;



WITH DelayInsights AS (
  SELECT 
    DATEPART(HOUR, timestamp) AS hour_of_day,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) AS delayed_transactions,
    SUM(TRY_CAST(User_Transaction_Amount AS FLOAT)) AS total_value,
    AVG(TRY_CAST(User_Transaction_Amount AS FLOAT)) AS avg_value
  FROM [dbo].[Logistics]
  WHERE ISNUMERIC(User_Transaction_Amount) = 1
  GROUP BY DATEPART(HOUR, timestamp)
),
DelayRateCalc AS (
  SELECT 
    hour_of_day,
    delayed_transactions,
    total_transactions,
    total_value,
    avg_value,
    CAST(delayed_transactions AS FLOAT) / total_transactions AS delay_rate
  FROM DelayInsights
)
SELECT 
  hour_of_day,
  delayed_transactions,
  total_transactions,
  FORMAT(delay_rate * 100, 'N2') + '%' AS delay_rate,
  FORMAT(total_value, 'N2') AS total_value,
  FORMAT(avg_value, 'N2') AS avg_transaction_value,
  CASE 
    WHEN CAST(delayed_transactions AS FLOAT) / total_transactions > 0.5 THEN 'Investigate logistics during this hour'
    WHEN avg_value < 10000 THEN ' Low-value transactions—consider customer incentives'
    ELSE 'Performance acceptable'
  END AS Recommendation
FROM DelayRateCalc
ORDER BY delay_rate DESC;




-- Create a temp table to hold intermediate results
WITH DailyStats AS (
  SELECT 
    DATENAME(WEEKDAY, timestamp) AS weekday,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) AS delayed_transactions,
    SUM(TRY_CAST(User_Transaction_Amount AS FLOAT)) AS total_value,
    AVG(TRY_CAST(User_Transaction_Amount AS FLOAT)) AS avg_value
  FROM [dbo].[Logistics]
  WHERE ISNUMERIC(User_Transaction_Amount) = 1
  GROUP BY DATENAME(WEEKDAY, timestamp)
),
DelayRateCalc AS (
  SELECT 
    weekday,
    delayed_transactions,
    total_transactions,
    total_value,
    avg_value,
    CAST(delayed_transactions AS FLOAT) / total_transactions AS delay_rate
  FROM DailyStats
)
SELECT 
  weekday,
  delayed_transactions,
  total_transactions,
  FORMAT(delay_rate * 100, 'N2') + '%' AS delay_rate,
  FORMAT(total_value, 'N2') AS total_value,
  FORMAT(avg_value, 'N2') AS avg_transaction_value
INTO WeekdayAnalysis -- Creates a temp table
FROM DelayRateCalc;

--View the results
SELECT * FROM WeekdayAnalysis



-- Delay by Location (Latitude & Longitude Clusters)

SELECT 
  ROUND(CAST(latitude AS NUMERIC(10,1)), 1) AS lat,
  ROUND(CAST(longitude AS NUMERIC(10,1)), 1) AS lon,
  SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) AS delayed,
  COUNT(*) AS total,
  CAST(ROUND(100.0 * SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS DECIMAL(6,2)) AS delay_rate
  INTO Delay_by_location
FROM [dbo].[Logistics]
GROUP BY ROUND(CAST(latitude AS NUMERIC(10,1)), 1), ROUND(CAST(longitude AS NUMERIC(10,1)), 1)
ORDER BY delay_rate DESC;



-- Predictive Signal Identification

SELECT 
  traffic_status,
  shipment_status,
  CASE 
    WHEN asset_utilization > 85.0 THEN 'High Util'
    WHEN asset_utilization > 50.0 THEN 'Medium Util'
    ELSE 'Low Util'
  END AS util_band,
  SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) AS delayed,
  COUNT(*) AS total,
  CAST(ROUND(100.0 * SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS DECIMAL(6,2)) AS delay_rate
--INTO logistics_summary_table
FROM Logistics
GROUP BY traffic_status, shipment_status,
  CASE 
    WHEN asset_utilization > 85.0 THEN 'High Util'
    WHEN asset_utilization > 50.0 THEN 'Medium Util'
    ELSE 'Low Util'
  END
ORDER BY delay_rate DESC;


-- How delay is affecting transaction

SELECT 
  CASE 
    WHEN logistics_delay = 1 THEN 'Delayed'
    ELSE 'On Time'
  END AS DelayStatus,
  COUNT(*) AS TotalTransactions,
  SUM(TRY_CAST(User_Transaction_Amount AS FLOAT)) AS TotalValue,
  AVG(TRY_CAST(User_Transaction_Amount AS FLOAT)) AS AverageValue,
  FORMAT(
    CAST(
      ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM Logistics), 2)
    AS DECIMAL(6,2)), 'N2') AS ShareOfTotal
FROM Logistics
WHERE ISNUMERIC(User_Transaction_Amount) = 1
GROUP BY logistics_delay
ORDER BY TotalTransactions DESC;

-- Aggregate delay and transaction data by weekday
WITH DelayStats AS (
  SELECT 
    DATENAME(WEEKDAY, timestamp) AS weekday,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN logistics_delay = 1 THEN 1 ELSE 0 END) AS delayed_transactions,
    AVG(TRY_CAST(User_Transaction_Amount AS FLOAT)) AS avg_transaction_value
  FROM [dbo].[Logistics]
  WHERE ISNUMERIC(User_Transaction_Amount) = 1 -- Ensure only numeric values are used
  GROUP BY DATENAME(WEEKDAY, timestamp)
),

-- Prepare inputs for correlation calculation
CorrelationInputs AS (
  SELECT 
    weekday,
    CAST(delayed_transactions AS FLOAT) / total_transactions AS delay_rate,
    avg_transaction_value
  FROM DelayStats
),

-- Calculate Pearson correlation coefficient
FinalStats AS (
  SELECT *,
    (SELECT AVG(delay_rate) FROM CorrelationInputs) AS dr_avg,
    (SELECT AVG(avg_transaction_value) FROM CorrelationInputs) AS tv_avg
  FROM CorrelationInputs
)

-- Output the correlation result
SELECT 
  SUM((delay_rate - dr_avg) * (avg_transaction_value - tv_avg)) / 
  (SQRT(SUM(POWER(delay_rate - dr_avg, 2))) * SQRT(SUM(POWER(avg_transaction_value - tv_avg, 2)))) AS correlation_coefficient
FROM FinalStats;

/* Result of the correlation between delay rate and avg transaction value is -0.718.

This shows that as delay rate increases, average transaction value tends to decrease.

When logistics delays are frequent (high delay rate), the value of transactions processed tends to be lower.

This could be due to:

Customers losing trust and choosing lower-value purchases.

Premium clients opting out during delay-heavy days.

Operational inefficiencies impacting high-value transactions.

*/














