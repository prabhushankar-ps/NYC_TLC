-- Set up NYC Taxi ingestion infrastructure with Parquet stage and raw table
-- Co-authored with CoCo
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS LOAD_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS NYC_TAXI;
USE DATABASE NYC_TAXI;
CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;
CREATE FILE FORMAT IF NOT EXISTS PARQUET_FF TYPE = PARQUET;
CREATE STAGE IF NOT EXISTS TLC_STAGE FILE_FORMAT = NYC_TAXI.RAW.PARQUET_FF;

CREATE TABLE IF NOT EXISTS RAW_YELLOW_TRIPS (
  VendorID               NUMBER,
  tpep_pickup_datetime   TIMESTAMP_NTZ,
  tpep_dropoff_datetime  TIMESTAMP_NTZ,
  passenger_count        FLOAT,
  trip_distance          FLOAT,
  RatecodeID             FLOAT,
  store_and_fwd_flag     VARCHAR,
  PULocationID           NUMBER,
  DOLocationID           NUMBER,
  payment_type           NUMBER,
  fare_amount            FLOAT,
  extra                  FLOAT,
  mta_tax                FLOAT,
  tip_amount             FLOAT,
  tolls_amount           FLOAT,
  improvement_surcharge  FLOAT,
  total_amount           FLOAT,
  congestion_surcharge   FLOAT,
  Airport_fee            FLOAT,
  _source_file           VARCHAR,
  _loaded_at             TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

