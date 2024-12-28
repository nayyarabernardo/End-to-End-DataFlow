WITH source AS (
	SELECT 
        category_id,
        category_department_id, 
        category_name
        
	FROM {{ source('db_retail', 'trusted_categories') }}
)

SELECT
	*
FROM source