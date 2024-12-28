WITH source AS (
	SELECT 
        department_id,
        department_name
        
	FROM {{ source('db_retail', 'trusted_departments') }}
)

SELECT
	*
FROM source