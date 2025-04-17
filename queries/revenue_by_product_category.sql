
WITH product_category_revenue AS (
	SELECT 
	product_category,
	SUM(amount) AS revenue
	FROM 
	diwali_sales
	GROUP BY product_category
)
SELECT 
	product_category,
	revenue,
    RANK() OVER(ORDER BY revenue DESC) AS revenue_rank
FROM 
    product_category_revenue;