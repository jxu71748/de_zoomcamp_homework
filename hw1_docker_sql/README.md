
docker-compose up -d

# Run locally to ingest data for green taxi
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"

python ingest_data_green_taxi.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=green_taxi_trips \
  --url=${URL}


  # Run locally to ingest data for green taxi
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

python ingest_data_zone.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=taxi_zone \
  --url=${URL}


# Terraform
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=data-engineer-zoomcamp-448404"

# Create new infra
terraform apply -var="project=data-engineer-zoomcamp-448404"

# Delete infra after your work, to avoid costs on any running services
terraform destroy


# 1.
docker pull python:3.12.8
docker run -it --entrypoint bash python:3.12.8

# 3. sql

SELECT
    COUNT(*) FILTER (WHERE trip_distance <= 1) AS up_to_1_mile,
    COUNT(*) FILTER (WHERE trip_distance > 1 AND trip_distance <= 3) AS between_1_and_3_miles,
    COUNT(*) FILTER (WHERE trip_distance > 3 AND trip_distance <= 7) AS between_3_and_7_miles,
    COUNT(*) FILTER (WHERE trip_distance > 7 AND trip_distance <= 10) AS between_7_and_10_miles,
    COUNT(*) FILTER (WHERE trip_distance > 10) AS over_10_miles
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01' AND lpep_pickup_datetime < '2019-11-01';



# 4. sql
SELECT
    DATE(lpep_pickup_datetime) AS pickup_date,
    MAX(trip_distance) AS max_trip_distance
FROM
    green_taxi_trips
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 1;


# 5. sql 
SELECT 
    tz."Zone" AS pickup_zone,
    SUM(gt.total_amount) AS total_amount
FROM 
    green_taxi_trips gt
JOIN 
    taxi_zone tz
ON 
    gt."PULocationID" = tz."LocationID"
WHERE 
    DATE(gt.lpep_pickup_datetime) = '2019-10-18'
GROUP BY 
    tz."Zone"
HAVING 
    SUM(gt.total_amount) > 13000
ORDER BY 
    total_amount DESC;


# 6. sql
SELECT 
    tz_dropoff."Zone" AS dropoff_zone,
    MAX(gt.tip_amount) AS max_tip
FROM 
    green_taxi_trips gt
JOIN 
    taxi_zone tz_pickup
ON 
    gt."PULocationID" = tz_pickup."LocationID"
JOIN 
    taxi_zone tz_dropoff
ON 
    gt."DOLocationID" = tz_dropoff."LocationID"
WHERE 
    DATE(gt.lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31'
    AND tz_pickup."Zone" = 'East Harlem North'
GROUP BY 
    tz_dropoff."Zone"
ORDER BY 
    max_tip DESC
LIMIT 1;


