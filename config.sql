--- This script is used as a backup for the RBAC hierarchy

--- Using super user account
USE ROLE ACCOUNTADMIN;


--- Creating the dbt_wh to do the transformations

DROP WAREHOUSE IF EXISTS STORI_CHALLENGE_WH;
CREATE WAREHOUSE STORI_CHALLENGE_WH WITH warehouse_size = 'x-small' auto_suspend = 60;


DROP DATABASE IF EXISTS STORI_CHALLENGE_DB;
--- Creating the database (where all the transformations happen)
CREATE DATABASE STORI_CHALLENGE_DB;

--- Creating the schemas that serve as the medallion architecture

CREATE SCHEMA STORI_CHALLENGE_BRONZE;


DROP ROLE IF EXISTS STORI_CHALLENGE_ROLE;
--- Creating the role to do the transformations
CREATE ROLE STORI_CHALLENGE_ROLE;

--- Grant the following permissions to the roles and users:
--  - usage of the warehouse to STORI_CHALLENGE_ROLE
--  - grant all permissions of the database to STORI_CHALLENGE_ROLE
--  - grant create tables on schemas to STORI_CHALLENGE_ROLE
--  - grant role to user

-- usage of the warehouse to STORI_CHALLENGE_ROLE
GRANT USAGE ON WAREHOUSE STORI_CHALLENGE_WH TO STORI_CHALLENGE_ROLE;


-- grant all permissions of the database to STORI_CHALLENGE_ROLE
GRANT ALL ON DATABASE STORI_CHALLENGE_DB TO STORI_CHALLENGE_ROLE;

-- grant create tables on schemas to STORI_CHALLENGE_ROLE
GRANT CREATE TABLE ON SCHEMA STORI_CHALLENGE_DB.STORI_CHALLENGE_BRONZE TO ROLE STORI_CHALLENGE_ROLE;


--- Grant usages of the schemas to STORI_CHALLENGE_ROLE
GRANT USAGE ON SCHEMA STORI_CHALLENGE_DB.STORI_CHALLENGE_BRONZE TO ROLE STORI_CHALLENGE_ROLE;


--- Grant create to stages of the schemas to STORI_CHALLENGE_ROLE
GRANT CREATE STAGE ON SCHEMA STORI_CHALLENGE_DB.STORI_CHALLENGE_BRONZE TO ROLE STORI_CHALLENGE_ROLE;


--- Grant all privileges of the schemas to STORI_CHALLENGE_ROLE
GRANT ALL PRIVILEGES ON SCHEMA STORI_CHALLENGE_DB.STORI_CHALLENGE_BRONZE TO ROLE STORI_CHALLENGE_ROLE;


-- Finally grant role to users

GRANT ROLE STORI_CHALLENGE_ROLE TO USER SHADOW750098;




-- And the stage
CREATE STAGE STORI_CHALLENGE_DB.STORI_CHALLENGE_BRONZE.STORI_STAGE;


-- Creating the stage table
CREATE OR REPLACE TABLE STORI_CHALLENGE_DB.STORI_CHALLENGE_BRONZE.STORI_BRONZE (
    id INT NOT NULL , --id del movimiento
    tittle VARCHAR(200),
    price DECIMAL,
    description     TEXT,            
    category        VARCHAR(200),
    image TEXT,
    rating_rate DECIMAL,
    rating_count DECIMAL
);


--- For dynamic table creation --- actually not working

ALTER TABLE STORI_CHALLENGE_DB.STORI_CHALLENGE_BRONZE.STORI_BRONZE SET ENABLE_SCHEMA_EVOLUTION = TRUE;


CREATE OR REPLACE FILE FORMAT STORI_CHALLENGE_BRONZE.CSV
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ;

---- Storage integration
CREATE STORAGE INTEGRATION S3_STORAGE_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3' 
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn' 
  STORAGE_ALLOWED_LOCATIONS = ('s3://bucket/')
  COMMENT = 'Storage integration for S3 data loading';


---- Stage creation

CREATE OR REPLACE STAGE S3_STORI_STAGE
  URL = 's3://bucket'
  STORAGE_INTEGRATION = S3_STORAGE_INTEGRATION;

LIST @S3_STORI_STAGE;


DESC INTEGRATION S3_STORAGE_INTEGRATION;



