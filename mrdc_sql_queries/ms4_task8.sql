SELECT ROUND(SUM(orders_table.product_quantity * dim_products.product_price):: numeric, 2) AS total_sales,
	store_type,
	country_code
FROM dim_store_details
JOIN orders_table
	ON dim_store_details.store_code = orders_table.store_code
JOIN dim_products
	ON orders_table.product_code = dim_products.product_code
WHERE country_code = 'DE'
GROUP BY dim_store_details.store_type,
	country_code
ORDER BY total_sales;