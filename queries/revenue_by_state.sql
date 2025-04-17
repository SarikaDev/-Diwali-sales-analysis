
WITH state_revenue AS (
	SELECT 
	state,
	SUM(amount) AS revenue
	FROM 
	diwali_sales
	GROUP BY state
)
SELECT 
	state,
	revenue,
    RANK() OVER(ORDER BY revenue DESC) AS revenue_rank
FROM 
    state_revenue;
