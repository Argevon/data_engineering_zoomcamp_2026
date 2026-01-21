SQL QUERIES HOMEWORK:

Question 3. For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile?

SELECT COUNT(*) AS short_trips
FROM green_tripdata_2025_11
WHERE trip_distance <= 1;

Question 4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles. (1 point) 

SELECT 
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS max_distance
FROM green_tripdata_2025_11
WHERE trip_distance < 100
GROUP BY pickup_day
ORDER BY max_distance DESC
LIMIT 1;

Question 5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025? (1

SELECT 
    tz."Zone" AS pickup_zone,
    SUM(g."total_amount") AS total_revenue
FROM green_tripdata_2025_11 g
JOIN taxi_zones tz
    ON g."PULocationID" = tz."LocationID"
WHERE DATE(g."lpep_pickup_datetime") = '2025-11-18'
GROUP BY tz."Zone"
ORDER BY total_revenue DESC
LIMIT 1;

Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip? (1 point) 

SELECT 
    tz_drop."Zone" AS dropoff_zone,
    g."tip_amount" AS tip_amount
FROM green_tripdata_2025_11 g
JOIN taxi_zones tz_pick
    ON g."PULocationID" = tz_pick."LocationID"
JOIN taxi_zones tz_drop
    ON g."DOLocationID" = tz_drop."LocationID"
WHERE tz_pick."Zone" = 'East Harlem North'
  AND g."lpep_pickup_datetime" >= '2025-11-01'
  AND g."lpep_pickup_datetime" < '2025-12-01'
ORDER BY g."tip_amount" DESC
LIMIT 1;
