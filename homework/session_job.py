from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_events_source_kafka(t_env):
    """
    convert `event_timestamp` to TIMESTAMP(3)。
    """
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_timestamp AS CAST(lpep_dropoff_datetime AS TIMESTAMP(3)),  -- 关键修正
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_events_aggregated_sink(t_env):
    table_name = "processed_trips_aggregated"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INT,
            DOLocationID INT,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            total_trips BIGINT,
            PRIMARY KEY (PULocationID, DOLocationID, session_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60 * 1000)  # start Checkpoint
    env.set_parallelism(1)  

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # create Kafka Source Table
        source_table = create_events_source_kafka(t_env)
        # create PostgreSQL Sink Table
        aggregated_table = create_events_aggregated_sink(t_env)
        
        # execute SQL to start streaming processing
        t_env.execute_sql(f"""
            INSERT INTO {aggregated_table}
            SELECT
                PULocationID,
                DOLocationID,
                SESSION_START(event_timestamp, INTERVAL '5' MINUTE) AS session_start,
                SESSION_END(event_timestamp, INTERVAL '5' MINUTE) AS session_end,
                COUNT(*) AS total_trips
            FROM {source_table}
            GROUP BY SESSION(event_timestamp, INTERVAL '5' MINUTE), PULocationID, DOLocationID;
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
