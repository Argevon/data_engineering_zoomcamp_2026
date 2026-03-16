from common import create_green_source, jdbc_options, make_table_env


def run():
    t_env = make_table_env(parallelism=1)
    source_table = create_green_source(t_env, startup_mode="earliest-offset")

    t_env.execute_sql(
        f"""
        CREATE TABLE q6_hourly_tip (
            window_start TIMESTAMP(3),
            total_tip DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) {jdbc_options('q6_hourly_tip')}
        """
    )

    t_env.execute_sql(
        f"""
        INSERT INTO q6_hourly_tip
        SELECT
            window_start,
            SUM(tip_amount) AS total_tip
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
        )
        GROUP BY window_start
        """
    ).wait()


if __name__ == "__main__":
    run()
