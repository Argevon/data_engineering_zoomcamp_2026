CREATE TABLE IF NOT EXISTS q4_tumbling_pu (
    window_start TIMESTAMP(3),
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

CREATE TABLE IF NOT EXISTS q5_session_pu (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);

CREATE TABLE IF NOT EXISTS q6_hourly_tip (
    window_start TIMESTAMP(3) PRIMARY KEY,
    total_tip DOUBLE PRECISION
);
