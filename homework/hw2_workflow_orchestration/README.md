### For the homework, we'll be working with the green taxi dataset 

## Assignment
So far in the course, we processed data for the year 2019 and 2020. This task is to extend the existing flows to include data from January to July for the year 2021.

Deploying 06_gcp_taxi_scheduled.yml to extend the existing flows for yellow and green taxi.

# 1. 
Go to GCP -> Cloud Storage -> Buckets
Looking for:
    Name: yellow_tripdata_2020-12.csv
Then we can see:
    Size: 128.3MB


# 2. 
Refer to 06_gcp_taxi.yaml then looking for:
variables:
  file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
So, after inputting the data in, we have green_tripdata_2020-04.csv


# 3. This question is asking for the Yellow taxi data for all CSV files in the year 2020, so we are using the csv filename to query
SELECT 
    COUNT(*) AS total_rows
FROM 
    data-engineer-zoomcamp-448404.de_zoomcamp.yellow_tripdata
WHERE 
    filename LIKE 'yellow_tripdata_2020-%'


# 4. similar to question 3
SELECT
  COUNT(*) AS total_rows_green_2020
FROM 
  data-engineer-zoomcamp-448404.de_zoomcamp.green_tripdata
WHERE
  filename LIKE 'green_tripdata_2020-%'


# 5. This question is asking for the total rows in yellow_tripdata_2021-03.csv file
SELECT  
  COUNT(*) AS total_rows
FROM 
  data-engineer-zoomcamp-448404.de_zoomcamp.yellow_tripdata
WHERE 
  filename LIKE 'yellow_tripdata_2021-03.csv'

