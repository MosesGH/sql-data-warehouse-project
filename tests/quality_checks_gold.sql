/*
====================================================================================
Quality Checks
====================================================================================
Script Purpose:
This script performs quality chgecks to calidate the integrity, consistency,
and accuracy of the gold layer. These checks ensure:
  - Uniqueness of surrogate keys
  - Referential integrity between fact and dimension tables
  - Validation of relationships in the data model for analytical purposes
Usage:
	- Run these checks after datra loading the Silver Layer.
  - Investigate and resolve and discrepancies found during the checks.

====================================================================================
*/


-- ==========================================================
-- 'checking gold.dim_customers'
-- ==========================================================  
--check for uniqueness of customer key in gold.dim_customers
--expectation: No results

SELECT
  customer_key,
  COUNT(*) AS duplicate_count
FROM gold.dim_customers
FGROUP BY customer_key
HAVING COUNT(*) > 1;

-- ==========================================================
-- 'checking gold.product_key'
-- ==========================================================  
--check for uniqueness of product key in gold.dim_products
--expectation: No results

SELECT
  product_key,
  COUNT(*) AS duplicate_count
FROM gold.dim_products
FGROUP BY customer_key
HAVING COUNT(*) > 1;

-- ==========================================================
-- 'checking gold.fact_sales'
-- ==========================================================  
--check the data model connectivity betweenm fact and dimension

SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_id IS NULL
