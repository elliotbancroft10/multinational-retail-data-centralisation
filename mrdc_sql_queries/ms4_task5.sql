WITH store_sales AS (
    SELECT 
        CASE
            WHEN location = 'Offline' THEN dim_store_details.store_type
            ELSE 'Web Portal'
        END AS "Store_Type",
        SUM(product_quantity * dim_products.product_price)::numeric AS total_sales
    FROM (
        SELECT *,
            CASE 
                WHEN store_code LIKE 'WEB%' THEN 'Web'
                ELSE 'Offline'
            END AS location
        FROM orders_table
    ) AS query_table
    LEFT JOIN dim_store_details
        ON query_table.store_code = dim_store_details.store_code
    JOIN dim_products
        ON query_table.product_code = dim_products.product_code
    GROUP BY "Store_Type"
)

SELECT 
    "Store_Type",
    ROUND(total_sales, 2) AS total_sales,
    ROUND((total_sales / SUM(total_sales) OVER ()) * 100, 2) AS "percentage_total(%)"
FROM 
    store_sales
ORDER BY total_sales DESC;