/*
====================================================================================
DDL SCript: Create Gold Views
====================================================================================
Script Purpose:
	This script creates ciews for the Gold layer ion the data warehouse.
	The Gold later represents the final dimensions and fact tables (Star Schema)

	Each view performs transformations and combines data from the Silver later to 
	produce a clean, enriched, and business-ready dataset

Usage:
	- These views can be queried directly for analytics and reporting. 

====================================================================================
*/

SELECT
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gnder,
ci.cst_create_date,
ca.erp_BDATE,
ca.erp_GEN,
la.erp_CNTRY
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.erp_CID -- as we have p[repped the data we can be confident of this join
LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.erp_CID


--After joing the tables, check if any duplicates were introduced by the join logic in case of poor data
SELECT CST_ID, COUNT(*) FROM
(
SELECT
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gnder,
ci.cst_create_date,
ca.erp_BDATE,
ca.erp_GEN,
la.erp_CNTRY
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.erp_CID -- as we have p[repped the data we can be confident of this join
LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.erp_CID
		) t GROUP BY CST_ID
		HAVING COUNT(*) > 1 -- THIS IS CHECKING FOR DUPLICATES IN THE PRIMARY TABLE

	-- THIS IS A VERY IMPORTANT CHECK. 

SELECT
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gnder,
ci.cst_create_date,
ca.erp_BDATE,
ca.erp_GEN,
la.erp_CNTRY
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.erp_CID -- as we have p[repped the data we can be confident of this join
LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.erp_CID-- there are two sources for the gender information, data must be integrated



SELECT
ci.cst_gnder,
ca.erp_GEN
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.erp_CID -- as we have p[repped the data we can be confident of this join
LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.erp_CID
ORDER BY 1,2

-- We removed NULL but they still appear as the join could not find a match from table 2 to table 1
-- Here we need to ask the experts which the master data is. which information is more accurate
-- we will say that cst_gnder is

SELECT DISTINCT
	ci.cst_gnder,
	ca.erp_GEN,
	CASE WHEN ci.cst_gnder != 'n/a' THEN ci.cst_gnder -- CRM is Master for Gender Info
		ELSE COALESCE (ca.erp_GEN, 'n/a') -- used tyo replace nulls in a join
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.erp_CID -- as we have p[repped the data we can be confident of this join
LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.erp_CID
ORDER BY 1,2

-- Here we have created a unified pievcce of information that is richer than it's parts. This is data integration. 
-- now we have this, we can take it to the original query. 


SELECT
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.erp_CNTRY AS country,  -- grouping relevant columns
	ci.cst_marital_status AS maritual_status,
	CASE WHEN ci.cst_gnder != 'n/a' THEN ci.cst_gnder -- CRM is Master for Gender Info
		ELSE COALESCE (ca.erp_GEN, 'n/a') -- used tyo replace nulls in a join
	END AS Gender,
	ca.erp_BDATE AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.erp_CID -- as we have p[repped the data we can be confident of this join
LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.erp_CID
-- three tables used to make on e object, now give it a friendly name for end users. 
--here all of the columns describe the customers so this is a dimension table
--when making a dimension you have to make a primary key in the data warehouse, sometimes it is obvious
--sometimes a new one must be made called a surrogate key, it is only used to connect the data model. 
--this  gives us more control and means we dont have to rely on the datamodel.
--Here we use the window function (Row_Number) instead of DDL generation.

/*
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

-- next check the quality of the object

SELECT * FROM gold.dim_customers
SELECT DISTINCT Gender FROM gold.dim_customers



--Building the product table
---targeting end date null as it is the most recent and current information
SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.Prd_cost,
pn.Prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.erp_CAT,
pc.erp_SUBCAT,
PC.erp_MAINTENANCE
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.erp_ID
WHERE prd_end_dt IS NULL -- filter out old historical data

-- NOW GOING TO CHECK THE UNIQUENESS as will use to join with the sales table.
SELECT prd_key, COUNT(*) FROM (
SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.Prd_cost,
pn.Prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.erp_CAT,
pc.erp_SUBCAT,
PC.erp_MAINTENANCE
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.erp_ID
WHERE prd_end_dt IS NULL
)t GROUP BY prd_key
HAVING COUNT(*) > 1 -- no duplicated created from joining. 
-- nothing to integrate as no duplicate columns

SELECT
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.erp_CAT AS category,
pc.erp_SUBCAT AS subcateggory,
PC.erp_MAINTENANCE AS maintenance,
pn.Prd_cost AS cost,
pn.Prd_line AS product_line,
pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.erp_ID
WHERE prd_end_dt IS NULL

-- lots of descriptions about the product, no transactions or events, keys, ids, each row
--describes one product, so this is a dimension


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
-- each product now has a primary key
-- next check that the view has been created properly and there are no errors. 


SELECT * FROM gold.dim_products


-- Building The sales table
Select 
sd.sls_ord_num,
sd.sls_prd_key,
sd.sls_cust_id,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
FROM silver.crm_sales_details sd
-- Only data from the crm so no need for integrations transformation etc
-- can see transactions, dates, measures and metrics, ids, so this is a fact
-- Going to replace the facts IDs with the dimensions surrogate keys to easily connect the facts with the dimensions
-- This is a Data Lookup

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

-- then check the quality of the view
SELECT * FROM gold.fact_sales

-- join the whole thing to find any errors
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_id IS NULL
