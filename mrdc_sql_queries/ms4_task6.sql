SELECT ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
	"year", 
	"month"
FROM dim_date_times
JOIN orders_table
	ON dim_date_times.date_uuid = orders_table.date_uuid
JOIN dim_products
	ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.year, dim_date_times.month
ORDER BY total_sales DESC
LIMIT 10;
