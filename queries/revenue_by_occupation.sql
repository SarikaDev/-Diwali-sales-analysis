
WITH occupation_revenue AS (
	SELECT 
	occupation,
	SUM(amount) AS revenue
	FROM 
	diwali_sales
	GROUP BY occupation
)
SELECT 
	occupation,
	revenue,
    RANK() OVER(ORDER BY revenue DESC) AS revenue_rank
FROM 
    occupation_revenue;