WITH source AS (
	SELECT 
        product_id,
        product_category_id, 
        product_name,
        product_description,
        product_price,
        product_image
        
	FROM {{ source('db_retail', 'trusted_products') }}
)

SELECT
	*
FROM source