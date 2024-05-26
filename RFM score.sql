WITH last_order_date AS (
  SELECT CustomerID,
         MAX(InvoiceDate) AS Last_order
  FROM `turing_data_analytics.rfm` AS main_table
  WHERE InvoiceDate >= '2010-12-01' AND InvoiceDate < '2011-12-02'  -- Includes all of 2011-12-01
  GROUP BY CustomerID
),

rfm_metrics AS (
  SELECT rfm.CustomerID AS CustomerID,
         DATE_DIFF(DATE('2011-12-01'), DATE(last_order_date.Last_order), DAY) AS Recency,
         COUNT(DISTINCT rfm.InvoiceNo) AS Frequency,
         SUM(rfm.Quantity * rfm.UnitPrice) AS Monetary
  FROM `turing_data_analytics.rfm` AS rfm
  JOIN last_order_date
  ON rfm.CustomerID = last_order_date.CustomerID
  WHERE rfm.CustomerID IS NOT NULL
    AND rfm.InvoiceNo NOT LIKE 'C%'
    AND rfm.Quantity != 0
    AND rfm.UnitPrice != 0
    AND rfm.InvoiceDate >= '2010-12-01' AND rfm.InvoiceDate < '2011-12-02'  -- Includes all of 2011-12-01
  GROUP BY rfm.CustomerID, last_order_date.Last_order
),

Quantiles AS (
  SELECT
    APPROX_QUANTILES(Monetary, 4)[OFFSET(1)] AS m25,
    APPROX_QUANTILES(Monetary, 4)[OFFSET(2)] AS m50,
    APPROX_QUANTILES(Monetary, 4)[OFFSET(3)] AS m75,
    APPROX_QUANTILES(Frequency, 4)[OFFSET(1)] AS f25,
    APPROX_QUANTILES(Frequency, 4)[OFFSET(2)] AS f50,
    APPROX_QUANTILES(Frequency, 4)[OFFSET(3)] AS f75,
    APPROX_QUANTILES(Recency, 4)[OFFSET(1)] AS r25,
    APPROX_QUANTILES(Recency, 4)[OFFSET(2)] AS r50,
    APPROX_QUANTILES(Recency, 4)[OFFSET(3)] AS r75
  FROM rfm_metrics
),

rfm_calculation AS (
  SELECT
    rfm_metrics.*,
    CASE
      WHEN rfm_metrics.Recency <= Quantiles.r25 THEN 4
      WHEN rfm_metrics.Recency <= Quantiles.r50 THEN 3
      WHEN rfm_metrics.Recency <= Quantiles.r75 THEN 2
      WHEN rfm_metrics.Recency > Quantiles.r75 THEN 1
    END AS r_score,
    CASE
      WHEN rfm_metrics.Frequency <= Quantiles.f25 THEN 1
      WHEN rfm_metrics.Frequency <= Quantiles.f50 THEN 2
      WHEN rfm_metrics.Frequency <= Quantiles.f75 THEN 3
      WHEN rfm_metrics.Frequency > Quantiles.f75 THEN 4
    END AS f_score,
    CASE
      WHEN rfm_metrics.Monetary <= Quantiles.m25 THEN 1
      WHEN rfm_metrics.Monetary <= Quantiles.m50 THEN 2
      WHEN rfm_metrics.Monetary <= Quantiles.m75 THEN 3
      WHEN rfm_metrics.Monetary > Quantiles.m75 THEN 4
    END AS m_score
  FROM
    rfm_metrics,
    Quantiles
)

SELECT
  CONCAT(r_score, f_score, m_score) AS rfm_score,--Created additional column with RFM score and how many customer are in each rfm group. Used information to segment customer.
  COUNT(CustomerID) AS n
FROM
  rfm_calculation
GROUP BY
  rfm_score
