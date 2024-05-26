/*
SELECT COUNT(user_pseudo_id), COUNT(distinct user_pseudo_id)
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'first_visit' AND event_date <= '20210124'*/ -- Check IF there are dublicate users
WITH total_customers AS (
SELECT
  DISTINCT user_pseudo_id,
  DATE_TRUNC(PARSE_DATE('%Y%m%d', MIN(event_date)), WEEK) AS Cohort_week
FROM
  `turing_data_analytics.raw_events`
WHERE
  PARSE_DATE('%Y%m%d', event_date) BETWEEN DATE('2020-11-01') AND DATE('2021-01-30')
GROUP BY
  user_pseudo_id
),
customers AS (
SELECT
  Cohort_week,
  COUNT(user_pseudo_id) AS Total_customers_in_cohort
FROM
  total_customers
WHERE
  Cohort_week BETWEEN DATE('2020-11-01') AND DATE('2021-01-30')
GROUP BY
  Cohort_week
),
revenue_raw AS (
SELECT
  user_pseudo_id,
  DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK) AS Purchase_week,
  SUM(purchase_revenue_in_usd) AS Total_spent
FROM
  `turing_data_analytics.raw_events`
WHERE
  event_name = 'purchase'
  AND PARSE_DATE('%Y%m%d', event_date) BETWEEN DATE('2020-11-01') AND DATE('2021-01-30')
GROUP BY
  user_pseudo_id,
  DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)
),
revenue AS (
SELECT
  total_customers.Cohort_week,
  revenue_raw.Purchase_week,
  revenue_raw.user_pseudo_id,
  revenue_raw.Total_spent,
  DATE_DIFF(revenue_raw.Purchase_week, total_customers.Cohort_week, WEEK) AS Week_number
FROM
  total_customers
JOIN
  revenue_raw
ON
  total_customers.user_pseudo_id = revenue_raw.user_pseudo_id
WHERE
  revenue_raw.Purchase_week >= total_customers.Cohort_week
  AND DATE_DIFF(revenue_raw.Purchase_week, total_customers.Cohort_week, WEEK) < 13
)
SELECT
customers.Cohort_week,
customers.Total_customers_in_cohort,
SUM(CASE WHEN revenue.Week_number = 0 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week0,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 1 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week1,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 2 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week2,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 3 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week3,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 4 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week4,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 5 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week5,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 6 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week6,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 7 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week7,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 8 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week8,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 9 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week9,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 10 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week10,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 11 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week11,
SUM(CASE WHEN revenue.Week_number BETWEEN 0 AND 12 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Average_Revenue_Per_customer_after_Week12
FROM
customers
LEFT JOIN
revenue
ON
customers.Cohort_week = revenue.Cohort_week
GROUP BY
customers.Cohort_week,
customers.Total_customers_in_cohort
ORDER BY
customers.Cohort_week; -- Same quaery as first part but in this query was added different calculation. Calculated how many revenue did we get from cohort after n week. To get this was used condition to calculate how many weeks was after start. 0 week means same week. 1 one week after stert. 0-2 period from beggining until 2 week and same logic for all weeks.
