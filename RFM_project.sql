WITH last_order_date AS (
  SELECT CustomerID,
         MAX(InvoiceDate) AS Last_order--find last order date
  FROM `turing_data_analytics.rfm` AS main_table
  WHERE InvoiceDate >= '2010-12-01' AND InvoiceDate < '2011-12-02'  -- Includes all of 2010-12-01 and 2011-12-01
  GROUP BY CustomerID
)

SELECT rfm.CustomerID AS CustomerID,
       DATE_DIFF(DATE('2011-12-01'), DATE(last_order_date.Last_order), DAY) AS Recency,--Calculate days diff
       COUNT(DISTINCT rfm.InvoiceNo) AS Frequency,-- how many difference orders customer have made
       SUM(rfm.Quantity * rfm.UnitPrice) AS Monetary -- Item qty * price
FROM `turing_data_analytics.rfm` AS rfm
JOIN last_order_date
ON rfm.CustomerID = last_order_date.CustomerID
WHERE rfm.CustomerID IS NOT NULL--To clear data because there was invoice without customerID
  AND rfm.InvoiceNo NOT LIKE 'C%'-- Removed return orders.
  AND rfm.Quantity != 0
  AND rfm.UnitPrice != 0 -- Clear data from wrong given information
  AND rfm.InvoiceDate >= '2010-12-01' AND rfm.InvoiceDate < '2011-12-02'  -- Includes all of 2010-12-01 and 2011-12-01
GROUP BY rfm.CustomerID, last_order_date.Last_order
ORDER BY Monetary DESC
