# Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-engineer-zoomcamp-448404.hw3.external_yellow_taxi_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://hw3_de_zoomcamp/yellow_tripdata_2024-*.parquet']
);


# Create a regular/materialized table from external table
CREATE OR REPLACE TABLE data-engineer-zoomcamp-448404.hw3.regular_yellow_taxi_2024 AS
SELECT * FROM data-engineer-zoomcamp-448404.hw3.external_yellow_taxi_2024;


# 1. 
open the regular_yellow_taxi_2024 table:
DETAILS -> Storage info -> Number of rows : 20,332,093

# 2. 
-- distinct number of PULocationIDs for external table
SELECT COUNT(DISTINCT(PULocationID))
FROM data-engineer-zoomcamp-448404.hw3.external_yellow_taxi_2024；

-- distinct number of PULocationIDs for materialized table
SELECT COUNT(DISTINCT(PULocationID))
FROM data-engineer-zoomcamp-448404.hw3.regular_yellow_taxi_2024；


# 3.
-- Retrieve the PULocationID from the regular table
SELECT PULocationID
FROM data-engineer-zoomcamp-448404.hw3.regular_yellow_taxi_2024；

-- Retrieve the PULocationID and DOLocationID on the same regular table
SELECT PULocationID, DOLocationID
FROM data-engineer-zoomcamp-448404.hw3.regular_yellow_taxi_2024；


# 4. 
SELECT COUNT(*)
FROM data-engineer-zoomcamp-448404.hw3.regular_yellow_taxi_2024；
WHERE fare_amount = 0;


# 5. Create a partition and cluster table
CREATE OR REPLACE TABLE data-engineer-zoomcamp-448404.hw3.yellow_taxi_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM data-engineer-zoomcamp-448404.hw3.external_yellow_taxi_2024;


# 6. retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive) 
-- for materialized table
SELECT DISTINCT VendorID
FROM data-engineer-zoomcamp-448404.hw3.regular_yellow_taxi_2024
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' ANd '2024-03-15';

-- for partitioned table
SELECT DISTINCT VendorID
FROM data-engineer-zoomcamp-448404.hw3.yellow_taxi_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' ANd '2024-03-15';


# 7.
GCP Bucket

# 8. 
True

# 9. 
SELECT COUNT(*)
FROM data-engineer-zoomcamp-448404.hw3.regular_yellow_taxi_2024;
-- this one I got bytes processed 0B, because COUNT(*) on a materialized table often retrieves precomputed row counts from metadata, and BigQuery use cached results.
