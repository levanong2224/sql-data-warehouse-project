/*
 * T-SQL (microsoft sql server)
BULK insert bronze.crm_cust_info
from '/Users/levan/Data Engineer stuff/DATA WAREHOUSE PROJECT/DW PROJECT PROPER/datasets/source_crm/cust_info.csv'
with (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
*/




/*
 * =====================================================================
 * 
 * Stored Procedure: Load Bronze Layer (Source -> Bronze)
 * 
 * =====================================================================
 * 
 * Script Purpose:
 * 
 * This stored procedure loads data into the 'bronze' schema from extrernal CSV files.
 * It performes the following actions:
 * - Truncates the brnze tables before loading data.
 * - Uses the 'COPY' command to load data from csv Files to bronze tables.
 */

-- Stored Procedure
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	duration_secs DOUBLE PRECISION;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
	batch_duration_secs DOUBLE PRECISION;
BEGIN
	batch_start_time := clock_timestamp();
	BEGIN
		RAISE NOTICE '==================================================';
		RAISE NOTICE 'LOADING BRONZE LAYER';
		RAISE NOTICE '==================================================';
	
	
	
		RAISE NOTICE '--------------------------------------------------';
		RAISE NOTICE 'LOADING CRM TABLES';
		RAISE NOTICE '--------------------------------------------------';	
	

	
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
		COPY bronze.crm_cust_info
		FROM '/Users/levan/Data Engineer stuff/DATA WAREHOUSE PROJECT/DW PROJECT PROPER/datasets/source_crm/cust_info.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		duration_secs := EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '>> Load Duration: %, Start: %, End: % ', duration_secs, start_time, end_time;
		RAISE NOTICE '>>--------------';
		
		

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
		COPY bronze.crm_prd_info
		FROM '/Users/levan/Data Engineer stuff/DATA WAREHOUSE PROJECT/DW PROJECT PROPER/datasets/source_crm/prd_info.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		duration_secs := EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '>> Load Duration: %, Start: %, End: % ', duration_secs, start_time, end_time;		
		RAISE NOTICE '>>--------------';

		
		
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
		COPY bronze.crm_sales_details
		FROM '/Users/levan/Data Engineer stuff/DATA WAREHOUSE PROJECT/DW PROJECT PROPER/datasets/source_crm/sales_details.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		duration_secs := EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '>> Load Duration: %, Start: %, End: % ', duration_secs, start_time, end_time;				
		RAISE NOTICE '>>--------------';


		
		RAISE NOTICE '--------------------------------------------------';
		RAISE NOTICE 'LOADING ERP TABLES';
		RAISE NOTICE '--------------------------------------------------';	
	
	
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
		COPY bronze.erp_cust_az12
		FROM '/Users/levan/Data Engineer stuff/DATA WAREHOUSE PROJECT/DW PROJECT PROPER/datasets/source_erp/CUST_AZ12.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		duration_secs := EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '>> Load Duration: %, Start: %, End: % ', duration_secs, start_time, end_time;				
		RAISE NOTICE '>>--------------';

		
		
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101
		FROM '/Users/levan/Data Engineer stuff/DATA WAREHOUSE PROJECT/DW PROJECT PROPER/datasets/source_erp/LOC_A101.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		duration_secs := EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '>> Load Duration: %, Start: %, End: % ', duration_secs, start_time, end_time;				
		RAISE NOTICE '>>--------------';

		
		
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2
		FROM '/Users/levan/Data Engineer stuff/DATA WAREHOUSE PROJECT/DW PROJECT PROPER/datasets/source_erp/PX_CAT_G1V2.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		duration_secs := EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '>> Load Duration: %, Start: %, End: % ', duration_secs, start_time, end_time;	
		RAISE NOTICE '>>--------------';



	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE '==================================================';
			RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			RAISE NOTICE '==================================================';
	end;
	batch_end_time := clock_timestamp();
	batch_duration_secs := EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
	RAISE NOTICE '>>--------------------------------------------------';
	RAISE NOTICE '>> Batch Load Duration: %, Start: %, End: % ', batch_duration_secs, batch_start_time, batch_end_time;
	RAISE NOTICE '>>--------------------------------------------------';
end;
$$;

-- call the stored procedure
CALL bronze.load_bronze();
