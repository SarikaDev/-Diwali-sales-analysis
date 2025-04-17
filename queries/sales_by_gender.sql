SELECT * FROM diwali_sales
LIMIT 5

WITH gender_revenue AS (
    SELECT 
        gender,
        SUM(amount) AS revenue
    FROM 
        diwali_sales
    GROUP BY gender
),
total_revenue AS (
    SELECT SUM(amount) AS total FROM diwali_sales
)
SELECT 
    g.gender,
    ROUND(g.revenue) AS revenue,
    ROUND(g.revenue::DECIMAL / t.total * 100, 2) AS overall_contribution_pct
FROM 
    gender_revenue g, total_revenue t



-- By this code we can find which gender contributes more revenue in diwali sales at the end we can understand that which gender rank high