WITH last_order_date AS (
  SELECT
    CustomerID,
    MAX(InvoiceDate) AS Last_order
  FROM
    `turing_data_analytics.rfm` AS main_table
  WHERE
    InvoiceDate >= '2010-12-01' AND InvoiceDate < '2011-12-02'  -- Includes all of 2011-12-01
  GROUP BY
    CustomerID
),

rfm_metrics_raw AS (
  SELECT
    rfm.CustomerID AS CustomerID,
    DATE_DIFF(DATE('2011-12-01'), DATE(last_order_date.Last_order), DAY) AS Recency,
    COUNT(DISTINCT rfm.InvoiceNo) AS Frequency,
    SUM(rfm.Quantity * rfm.UnitPrice) AS Monetary
  FROM
    `turing_data_analytics.rfm` AS rfm
  JOIN
    last_order_date ON rfm.CustomerID = last_order_date.CustomerID
  WHERE
    rfm.CustomerID IS NOT NULL
    AND rfm.InvoiceNo NOT LIKE 'C%'
    AND rfm.Quantity != 0
    AND rfm.UnitPrice != 0
    AND rfm.InvoiceDate >= '2010-12-01' AND rfm.InvoiceDate < '2011-12-02'  -- Includes all of 2011-12-01
  GROUP BY
    rfm.CustomerID, last_order_date.Last_order
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
  FROM
    rfm_metrics_raw
),

rfm_metrics AS (
  SELECT
    CustomerID,
    Recency,
    Frequency,
    Monetary
  FROM
    rfm_metrics_raw
  WHERE
    Recency BETWEEN (SELECT r25 - 1.5 * (r75 - r25) FROM Quantiles) AND (SELECT r75 + 1.5 * (r75 - r25) FROM Quantiles) -- added filter to remove all outliners
    AND Frequency BETWEEN (SELECT f25 - 1.5 * (f75 - f25) FROM Quantiles) AND (SELECT f75 + 1.5 * (f75 - f25) FROM Quantiles)
    AND Monetary BETWEEN (SELECT m25 - 1.5 * (m75 - m25) FROM Quantiles) AND (SELECT m75 + 1.5 * (m75 - m25) FROM Quantiles)
),

RFM_Scores AS (
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
    rfm_metrics
  CROSS JOIN
    Quantiles
)

SELECT -- final table for visualization to grouped all customers to segments according their rfm score
  *,
  CONCAT(r_score, f_score, m_score) AS rfm_score,
  CASE
    WHEN CONCAT(r_score, f_score, m_score) IN ('443', '444') THEN 'Best Customers'
    WHEN CONCAT(r_score, f_score, m_score) IN ('324', '334', '344', '424', '434') THEN 'Big spenders'
    WHEN CONCAT(r_score, f_score, m_score) IN ('222', '223', '224', '321', '322', '323', '331', '332', '333', '341', '342', '421', '422', '423') THEN 'Discount Seek'
    WHEN CONCAT(r_score, f_score, m_score) IN ('121', '122', '123', '124', '131', '132', '133', '134', '141', '142', '142', '143', '144', '231', '232', '233', '234', '241', '242', '243', '244') THEN 'Lost Customer'
    WHEN CONCAT(r_score, f_score, m_score) IN ('343', '431', '432', '433', '441', '442') THEN 'Loyal Customer'
    WHEN CONCAT(r_score, f_score, m_score) IN ('311', '312', '313', '314', '411', '412', '413', '414') THEN 'Promising Prospects'
    WHEN CONCAT(r_score, f_score, m_score) IN ('111', '112', '113', '114', '211', '212', '213', '214', '221') THEN 'Trial Runners'
  END AS Segment
FROM
  RFM_Scores;
