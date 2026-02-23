import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import duckdb
    import pandas as pd
    import altair as alt
    import marimo as mo
    return alt, duckdb, mo, pd


@app.cell
def _(duckdb):
    conn = duckdb.connect("taxi_pipeline.duckdb", read_only=True)
    return (conn,)


@app.cell
def _(conn):
    row_count = conn.execute("SELECT COUNT(*) AS total_rows FROM taxi_data.trips").df()
    return (row_count,)


@app.cell
def _(mo, row_count):
    mo.md(f"## Total rows in taxi_data.trips: **{int(row_count.iloc[0, 0]):,}**")
    return


@app.cell
def _(conn):
    date_range = conn.execute(
        """
        SELECT
            MIN(trip_pickup_date_time) AS min_pickup,
            MAX(trip_pickup_date_time) AS max_pickup
        FROM taxi_data.trips
        """
    ).df()
    return (date_range,)


@app.cell
def _(date_range, mo):
    mo.md(
        f"""
### Pickup date range
- Min: **{date_range.loc[0, 'min_pickup']}**
- Max: **{date_range.loc[0, 'max_pickup']}**
"""
    )
    return


@app.cell
def _(conn):
    monthly_trips = conn.execute(
        """
        SELECT
            DATE_TRUNC('month', trip_pickup_date_time) AS pickup_month,
            COUNT(*) AS trips
        FROM taxi_data.trips
        WHERE trip_pickup_date_time IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """
    ).df()
    return (monthly_trips,)


@app.cell
def _(alt, monthly_trips):
    monthly_chart = (
        alt.Chart(monthly_trips)
        .mark_bar()
        .encode(
            x=alt.X("pickup_month:T", title="Month"),
            y=alt.Y("trips:Q", title="Trips"),
            tooltip=["pickup_month:T", "trips:Q"],
        )
        .properties(title="Monthly trip volume")
    )
    return (monthly_chart,)


@app.cell
def _(monthly_chart):
    monthly_chart
    return


@app.cell
def _(conn):
    payment_mix = conn.execute(
        """
        SELECT
            COALESCE(payment_type, 'UNKNOWN') AS payment_type,
            COUNT(*) AS trips
        FROM taxi_data.trips
        GROUP BY 1
        ORDER BY trips DESC
        """
    ).df()
    return (payment_mix,)


@app.cell
def _(alt, payment_mix):
    payment_chart = (
        alt.Chart(payment_mix)
        .mark_bar()
        .encode(
            x=alt.X("payment_type:N", sort="-y", title="Payment type"),
            y=alt.Y("trips:Q", title="Trips"),
            tooltip=["payment_type:N", "trips:Q"],
        )
        .properties(title="Payment type distribution")
    )
    return (payment_chart,)


@app.cell
def _(payment_chart):
    payment_chart
    return


@app.cell
def _(conn):
    sample_rows = conn.execute(
        """
        SELECT
            vendor_name,
            trip_pickup_date_time,
            trip_dropoff_date_time,
            trip_distance,
            fare_amt,
            tip_amt,
            total_amt
        FROM taxi_data.trips
        ORDER BY trip_pickup_date_time DESC NULLS LAST
        LIMIT 10
        """
    ).df()
    return (sample_rows,)


@app.cell
def _(sample_rows):
    sample_rows
    return


if __name__ == "__main__":
    app.run()
