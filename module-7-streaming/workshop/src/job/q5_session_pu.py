from common import create_green_source, jdbc_options, make_table_env


def run():
    t_env = make_table_env(parallelism=1)
    source_table = create_green_source(t_env, startup_mode="earliest-offset")

    t_env.execute_sql(
        f"""
        CREATE TABLE q5_session_pu (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
        ) {jdbc_options('q5_session_pu')}
        """
    )

    t_env.execute_sql(
        f"""
        INSERT INTO q5_session_pu
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID
        """
    ).wait()


if __name__ == "__main__":
    run()
