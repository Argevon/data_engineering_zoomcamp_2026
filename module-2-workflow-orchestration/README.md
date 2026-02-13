Question 3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020? 

SELECT COUNT(*) AS total_rows
FROM `YOUR_GCP_PROJECT_ID.YOUR_DATASET.yellow_tripdata`
WHERE filename LIKE 'yellow_tripdata_2020-%.csv';

Question 4. How many rows are there for the Green Taxi data for all CSV files in the year 2020? 

SELECT COUNT(*) AS total_rows
FROM `YOUR_GCP_PROJECT_ID.YOUR_DATASET.green_tripdata`
WHERE filename LIKE 'green_tripdata_2020-%.csv';

Question 5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

SELECT COUNT(*) AS total_rows
FROM `YOUR_GCP_PROJECT_ID.YOUR_DATASET.yellow_tripdata`
WHERE filename = 'yellow_tripdata_2021-03.csv';

