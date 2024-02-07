-- This query is reading parquet files from google cloud storage, and then create an external table.
CREATE OR REPLACE EXTERNAL TABLE
  `zmcp-413216.my_taxi.green_taxi_2022_external_table_v2` OPTIONS( format = 'PARQUET',
    uris = ['gs://mage-zmcp/green_taxi_data_wk3_v2/lpep_pickup_date=2022-*.parquet',
    'gs://mage-zmcp/green_taxi_data_wk3_v2/lpep_pickup_date=2021-*.parquet',
    'gs://mage-zmcp/green_taxi_data_wk3_v2/lpep_pickup_date=2008-*.parquet',
    'gs://mage-zmcp/green_taxi_data_wk3_v2/lpep_pickup_date=2009-*.parquet'] );

-- This query is to create the materialized table from the external table that we just created.
CREATE OR REPLACE TABLE
  `zmcp-413216.my_taxi.green_taxi_2022_export_v2` AS

  SELECT
  *
FROM
  zmcp-413216.my_taxi.green_taxi_2022_external_table_v2;


  -- Question 1: What is count of records for the 2022 Green Taxi Data??

SELECT
  COUNT(*)
FROM
  zmcp-413216.my_taxi.green_taxi_2022_external_table_v2;
  
-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT
  DISTINCT PULocationID
FROM
  zmcp-413216.my_taxi.green_taxi_2022_external_table_v2;

SELECT
  DISTINCT PULocationID
FROM
  zmcp-413216.my_taxi.green_taxi_2022_export_v2;
  

--Question 3: How many records have a fare_amount of 0?

SELECT
  COUNT(*)
FROM
  zmcp-413216.my_taxi.green_taxi_2022_external_table_v2
WHERE
  fare_amount=0;

  --Q5 Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
  
-- Need to change the lpep_pickup_datetime before it could be used as a date variable to used in partition by.
CREATE OR REPLACE TABLE
  zmcp-413216.my_taxi.green_taxi_2022_external_table_v2_partitioned
PARTITION BY
  date_column
CLUSTER BY
  PULocationID AS
SELECT
  DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', lpep_pickup_datetime_v2)) AS date_column,
  *
FROM
  zmcp-413216.my_taxi.green_taxi_2022_external_table_v2;


SELECT
  DISTINCT PULocationID
FROM
  zmcp-413216.my_taxi.green_taxi_2022_external_table_v2_partitioned
WHERE
  date_column BETWEEN '2022-06-01'
  AND '2022-06-30';

SELECT
  DISTINCT PULocationID
FROM
  zmcp-413216.my_taxi.green_taxi_2022_export_v2
WHERE
  lpep_pickup_datetime_v2 BETWEEN '2022-06-01'
  AND '2022-06-30'
