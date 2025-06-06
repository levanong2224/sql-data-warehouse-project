/*
 * 
 * ==================================================
 * 
 * DDL Script: Create Bronze Tables
 * 
 * ==================================================
 * 
 * Script Purpose: 
 * 
 * This Script creates tables in the 'bronze' schema, 
 * dropping existing tables if they already exists.
 * 
 * Run this Script to re-define the DDL Structure of
 * 'bronze' Tables
 * 
 * ===================================================
 */


-- Drop the table if it exists
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_material_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);



-- Drop the table if it exists
DROP TABLE IF EXISTS bronze.crm_sales_details;
create table bronze.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quanitity INT,
	sls_price INT
);



-- Drop the table if it exists
DROP TABLE IF EXISTS bronze.crm_prd_info;
create table bronze.crm_prd_info(
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt VARCHAR(50),
	prd_end_dt VARCHAR(50)
);



-- Drop the table if it exists
DROP TABLE IF EXISTS bronze.erp_loc_a101;
create table bronze.erp_loc_a101 (
	cid VARCHAR(50),
	cntry VARCHAR(50)
);



-- Drop the table if it exists
DROP TABLE IF EXISTS bronze.erp_cust_az12;
create table bronze.erp_cust_az12 (
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50)
);



-- Drop the table if it exists
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
);
