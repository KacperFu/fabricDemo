CREATE VIEW dbo.vw_sales_by_month AS
SELECT d.[Year], d.[Month], SUM(f.Revenue) AS Revenue
FROM dbo.fact_sales f
JOIN dbo.dim_date d ON f.OrderDate = d.[Date]
GROUP BY d.[Year], d.[Month];

GO