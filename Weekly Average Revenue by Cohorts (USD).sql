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
),-- CTE to get unique customers and earliest event that they have made in our website that was taken as registration
customers AS (
SELECT
  Cohort_week,
  COUNT(user_pseudo_id) AS Total_customers_in_cohort
FROM
  total_customers
WHERE
  Cohort_week BETWEEN DATE('2020-11-01') AND DATE('2021-01-30')
GROUP BY
  Cohort_week -- CTE to calculate how many customers was in each cohort according to out weekly cohorts.
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
  DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)-- CTE to get data of customers with their ID and when they have made purchase by cohort dates and total sum in that cohort is there would be more different purshases by same customer. ID wild be used latet to connect data from customer first evnet data with spending data.
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
)--CTE to connect data with first_event data with purchase data to have table with cohort week, customer ID and how many that customer have spend in that cohort. And calculated week difference between firts event cohort and purchase cohort to know when in which week after first event customer have made purchase.
SELECT
customers.Cohort_week,
customers.Total_customers_in_cohort,
SUM(CASE WHEN revenue.Week_number = 0 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week0_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 1 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week1_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 2 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week2_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 3 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week3_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 4 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week4_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 5 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week5_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 6 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week6_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 7 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week7_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 8 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week8_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 9 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week9_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 10 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week10_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 11 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week11_Spending_per_customer,
SUM(CASE WHEN revenue.Week_number = 12 THEN revenue.Total_spent ELSE 0 END) / customers.Total_customers_in_cohort AS Week12_Spending_per_customer
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
customers.Cohort_week;-- Final query to have calculate how many customer have sprend after n week after registration by cohort week. LEFT join used to get all customers that was at start date, but maybe did not make purchase in some week but we still would have this information.
