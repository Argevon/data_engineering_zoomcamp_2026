import requests
import dlt


BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
PAGE_SIZE = 1000


@dlt.resource(
    name="trips",
    write_disposition="append",
    columns={
        "rate_code": {"data_type": "text"},
        "mta_tax": {"data_type": "double"},
    },
)
def trips(base_url: str = BASE_URL, start_page: int = 1):
    page = start_page

    while True:
        response = requests.get(base_url, params={"page": page}, timeout=30)
        response.raise_for_status()
        records = response.json()

        if not records:
            break

        yield records

        if len(records) < PAGE_SIZE:
            break

        page += 1


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data",
    )
    load_info = pipeline.run(trips())
    print(load_info)


if __name__ == "__main__":
    run()
