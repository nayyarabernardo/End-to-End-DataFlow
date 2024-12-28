WITH source AS (
	SELECT 
        customer_id,
        CONCAT(customer_fname, ' ', customer_lname) AS customer_full_name, 
        customer_email,
        customer_password,
        customer_street,
        customer_city,
        customer_state,
        customer_zipcode
        
	FROM {{ source('db_retail', 'trusted_customers') }}
)

SELECT
	*
FROM source