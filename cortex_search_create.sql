USE DATABASE cortex_analyst_demo;
USE SCHEMA revenue_timeseries;

CREATE OR REPLACE DYNAMIC TABLE product_landing_table
  WAREHOUSE = cortex_analyst_wh
  TARGET_LAG = '1 hour'
  AS (
      SELECT DISTINCT product_line AS product_dimension FROM product_dim
  );


  CREATE OR REPLACE CORTEX SEARCH SERVICE product_line_search_service
  ON product_dimension
  WAREHOUSE = xsmall
  TARGET_LAG = '1 hour'
  AS (
      SELECT product_dimension FROM product_landing_table
  );`