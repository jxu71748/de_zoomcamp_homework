DROP TABLE IF EXISTS processed_trips_aggregated;

CREATE TABLE processed_trips_aggregated (
    PULocationID INT,
    DOLocationID INT,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    total_trips BIGINT,
    PRIMARY KEY (PULocationID, DOLocationID, session_start)
);

--TRUNCATE TABLE processed_trips_aggregated;
