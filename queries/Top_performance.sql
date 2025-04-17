-- this can find which age-group ppl on specific gender generated highest revenue

WITH gender_revenue AS (
    SELECT 
	gender,
		age_group,
        SUM(amount) AS revenue
    FROM 
        diwali_sales
    GROUP BY gender,age_group
),
total_revenue AS (
    SELECT SUM(amount) AS total FROM diwali_sales
)
SELECT 
    g.gender,
	g.age_group,
    ROUND(g.revenue) AS revenue,
    ROUND(g.revenue::DECIMAL / t.total * 100, 2) AS overall_contribution_pct,
	RANK() OVER (ORDER BY g.revenue DESC) AS revenue_rank
FROM 
    gender_revenue g, total_revenue t
