/*
====================================================================================
DDL SCript: Create Gold Views
====================================================================================
Script Purpose:
	This script creates views for the Gold layer ion the data warehouse.
	The Gold later represents the final dimensions and fact tables (Star Schema)

	Each view performs transformations and combines data from the Silver later to 
	produce a clean, enriched, and business-ready dataset

Usage:
	- These views can be queried directly for analytics and reporting. 

====================================================================================

====================================================================================
Creating the first view
====================================================================================
*/

Create VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- gives us the new key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.erp_CNTRY AS country,  -- grouping relevant columns
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gnder != 'n/a' THEN ci.cst_gnder -- CRM is Master for Gender Info
		ELSE COALESCE (ca.erp_GEN, 'n/a') -- used tyo replace nulls in a join
	END AS gender,
	ca.erp_BDATE AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.erp_CID -- as we have p[repped the data we can be confident of this join
LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.erp_CID

/*
====================================================================================
Creating the second view
====================================================================================
*/
CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt , pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.erp_CAT AS category,
pc.erp_SUBCAT AS subcategory,
PC.erp_MAINTENANCE AS maintenance,
pn.Prd_cost AS cost,
pn.Prd_line AS product_line,
pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.erp_ID
WHERE prd_end_dt IS NULL

/*
====================================================================================
Creating the third view
====================================================================================
*/
CREATE VIEW gold.fact_sales AS
Select 
sd.sls_ord_num AS order_number,
pr.product_key, -- we dont want the oroiginal, we want the surrogate that we have generated
cu.customer_key, -- again, same thing
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
