-- Run this once in a Snowflake worksheet before the first pipeline run.

CREATE WAREHOUSE IF NOT EXISTS SCD2_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS SCD2_DB;

USE DATABASE SCD2_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

CREATE TABLE IF NOT EXISTS DIM_PRODUCT_SCD2 (
    surrogate_key   NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    product_id      STRING NOT NULL,
    product_name    STRING,
    category        STRING,
    price           NUMBER(10, 2),
    supplier        STRING,
    row_hash        STRING NOT NULL,
    effective_date  DATE NOT NULL,
    end_date        DATE,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS SCD2_STAGING (
    change_type     STRING NOT NULL,  -- 'EXPIRE' or 'INSERT'
    product_id      STRING NOT NULL,
    product_name    STRING,
    category        STRING,
    price           NUMBER(10, 2),
    supplier        STRING,
    row_hash        STRING,
    effective_date  DATE,
    end_date        DATE
);
