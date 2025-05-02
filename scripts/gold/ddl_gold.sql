 /* 
 * ==================================================
 * 
 * DDL Script: Create Gold Views
 * 
 * ==================================================
 * 
 * Script Purpose: 
 * 
 * This Script creates views for the Gold Layer in the
 * data warehouse, dropping existing Views if they already exists.
 * 
 * The Gold Layer represents the final dimension and 
 * fact tables (Star Schema)
 *
 * 
 * Run this Script to re-define the DDL Structure of
 * 'Gold' Views
 * 
 * =====================================================
 */
 
  -- Drop in dependency order
DROP VIEW IF EXISTS gold.fact_sales;
DROP VIEW IF EXISTS gold.dim_customers;
DROP VIEW IF EXISTS gold.dim_products;
 
 -- =====================================================
 -- Create Dimension: gold.dim_customers
 -- =====================================================
 
CREATE VIEW gold.dim_customers AS 
SELECT 
	row_number() OVER (ORDER BY cst_id) AS customer_key,
	cci.cst_id AS customer_id,
	cci.cst_key AS customer_number,
	cci.cst_firstname AS first_name,
	cci.cst_lastname AS last_name,
	ela.cntry AS country,
	cci.cst_marital_status AS marital_status,
	CASE WHEN cst_gndr != 'n/a' THEN cci.cst_gndr -- CRM IS THE MASTER FOR GENDER INFO
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	cci.cst_create_date AS create_date
FROM silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 ca
ON cci.cst_key = ca.cid 
LEFT JOIN silver.erp_loc_a101 ela 
ON cci.cst_key = ela.cid;
 

 -- =====================================================
 -- Create Dimension: gold.dim_products
 -- =====================================================

CREATE VIEW gold.dim_products AS 
SELECT 
	row_number() OVER (ORDER BY cpi.prd_start_dt, cpi.prd_key) AS product_key,
	cpi.prd_id AS product_id,
	cpi.prd_key AS product_number,
	cpi.prd_nm AS product_name,
	cpi.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	cpi.prd_cost AS cost,
	cpi.prd_line AS product_line,
	cpi.prd_start_dt AS start_date
FROM silver.crm_prd_info cpi 
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON cpi.cat_id = pc.id
WHERE prd_end_dt IS NULL; --FILTER OUT ALL historical DATA

 -- =====================================================
 -- Create Dimension: gold.fact_sales
 -- =====================================================

CREATE VIEW gold.fact_sales AS 
SELECT 
sd.sls_ord_num AS order_number,  
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_gate, 
sd.sls_ship_dt AS shipping_date,  
sd.sls_due_dt AS due_date,  
sd.sls_sales AS sales_amount, 
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd 
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;
