/*--
• Database, schema, warehouse, and stage creation
--*/

USE ROLE SECURITYADMIN;

CREATE ROLE cortex_user_role;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE cortex_user_role; // should be granted to PUBLIC role already - may be superfluous

-- TODO: Replace <your_user> with your username
    // Add role to pertinent users for testing.
GRANT ROLE cortex_user_role TO USER KBAILEY;
GRANT ROLE cortex_user_role TO USER PSCHURRER;
GRANT ROLE cortex_user_role TO USER JVINCENT;
GRANT ROLE cortex_user_role TO USER JHUGHES;

USE ROLE sysadmin;

-- Create demo database
CREATE OR REPLACE DATABASE cortex_analyst_demo;

-- Create schema
CREATE OR REPLACE SCHEMA cortex_analyst_demo.revenue_timeseries;

-- Create warehouse
CREATE OR REPLACE WAREHOUSE cortex_analyst_wh
    WAREHOUSE_SIZE = 'small' //'large' not sure why it needs to be large
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Warehouse for Cortex Analyst demo';

GRANT USAGE ON WAREHOUSE cortex_analyst_wh TO ROLE cortex_user_role;
GRANT OPERATE ON WAREHOUSE cortex_analyst_wh TO ROLE cortex_user_role;

 GRANT OWNERSHIP ON SCHEMA cortex_analyst_demo.revenue_timeseries TO ROLE cortex_user_role; // In prod environment, would not be set up like this
 GRANT OWNERSHIP ON DATABASE cortex_analyst_demo TO ROLE cortex_user_role;                  // same issue here


USE ROLE cortex_user_role;

-- Use the created warehouse
USE WAREHOUSE cortex_analyst_wh;

USE DATABASE cortex_analyst_demo;
USE SCHEMA cortex_analyst_demo.revenue_timeseries;

-- Create stage for raw data
CREATE OR REPLACE STAGE raw_data DIRECTORY = (ENABLE = TRUE);

/*--
• Fact and Dimension Table Creation
--*/

-- Fact table: daily_revenue
CREATE OR REPLACE TABLE cortex_analyst_demo.revenue_timeseries.daily_revenue (
    date DATE,
    revenue FLOAT,
    cogs FLOAT,
    forecasted_revenue FLOAT,
    product_id INT,
    region_id INT
);

-- Dimension table: product_dim
CREATE OR REPLACE TABLE cortex_analyst_demo.revenue_timeseries.product_dim (
    product_id INT,
    product_line VARCHAR(16777216)
);

-- Dimension table: region_dim
CREATE OR REPLACE TABLE cortex_analyst_demo.revenue_timeseries.region_dim (
    region_id INT,
    sales_region VARCHAR(16777216),
    state VARCHAR(16777216)
);
use role accountadmin;

-- create a Git API integration for Snowflake Labs
-- This integration allows access to GitHub repositories under Snowflake-Labs
-- It is used for accessing demo data and scripts from the Snowflake Labs GitHub organization
CREATE OR REPLACE API INTEGRATION snowflake_labs_git_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs/')
  ENABLED = TRUE;

USE ROLE cortex_user_role;
-- Create a schema for Git repositories
-- This schema will contain Git repositories for the Cortex Analyst demo
CREATE OR REPLACE SCHEMA cortex_analyst_demo.git_repos;

-- Create a Git repository for the Cortex Analyst demo
-- This repository contains scripts and data for the Cortex Analyst demo
CREATE OR REPLACE GIT REPOSITORY cortex_analyst_demo.git_repos.getting_started_with_cortex_analyst
  API_INTEGRATION = snowflake_labs_git_integration
  ORIGIN = 'https://github.com/Snowflake-Labs/sfguide-getting-started-with-cortex-analyst';

-- Fetch the latest content from the Git repository
ALTER GIT REPOSITORY cortex_analyst_demo.git_repos.getting_started_with_cortex_analyst FETCH;
