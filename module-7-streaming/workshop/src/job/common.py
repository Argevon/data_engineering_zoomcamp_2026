from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment


def make_table_env(parallelism: int = 1) -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(parallelism)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    return StreamTableEnvironment.create(env, environment_settings=settings)


def create_green_source(t_env: StreamTableEnvironment, startup_mode: str = "earliest-offset") -> str:
    table_name = "green_events"
    ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = '{startup_mode}',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        )
    """
    t_env.execute_sql(ddl)
    return table_name


def jdbc_options(table_name: str) -> str:
    return f"""
        WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """
