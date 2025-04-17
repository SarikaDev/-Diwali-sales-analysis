SELECT * FROM diwali_sales
LIMIT 10

-- revenue
SELECT  SUM(amount) FROM diwali_sales

-- total Orders
SELECT  COUNT(orders) FROM diwali_sales

-- Average Order Value

WITH sales_summary AS (
    SELECT 
        SUM(amount)::INT AS revenue,
        COUNT(orders)::INT AS orders
    FROM diwali_sales
)

SELECT 
    revenue,
    orders,
    ROUND((revenue::NUMERIC / NULLIF(orders, 0)), 0)::INT AS aov
FROM sales_summary;

-- Female Head Count
SELECT  COUNT(gender) FROM diwali_sales
WHERE gender ='Female'


-- Male Head Count
SELECT  COUNT(gender) FROM diwali_sales
WHERE gender ='Male'
