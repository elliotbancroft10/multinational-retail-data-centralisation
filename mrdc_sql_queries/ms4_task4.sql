SELECT COUNT(*) AS number_of_sales,
	SUM(product_quantity) AS product_quantity_count,
	location	
FROM (
	SELECT *,
       CASE 
         WHEN store_code LIKE 'WEB%' THEN 'Web'
         ELSE 'Offline'
       END AS location
	FROM orders_table
) AS query_table
GROUP BY location; 
	