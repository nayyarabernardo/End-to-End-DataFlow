WITH source AS (
	SELECT 
        order_item_id,
        order_item_order_id, 
        order_item_product_id,
        order_item_quantity,
        order_item_subtotal,
        order_item_product_price
        
	FROM {{ source('db_retail', 'trusted_order_items') }}
)

SELECT
	*
FROM source