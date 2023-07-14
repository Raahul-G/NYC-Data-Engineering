-- Create datetime_table_2020
CREATE TABLE uber-de-project-raahul.Uber_de_bq.datetime_table_2020 AS
SELECT
    ROW_NUMBER() OVER (ORDER BY tpep_pickup_datetime) AS datetime_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS pick_hour,
    EXTRACT(DAY FROM tpep_pickup_datetime) AS pick_day,
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS pick_month,
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS pick_year,
    EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) AS pick_weekday,
    EXTRACT(HOUR FROM tpep_dropoff_datetime) AS drop_hour,
    EXTRACT(DAY FROM tpep_dropoff_datetime) AS drop_day,
    EXTRACT(MONTH FROM tpep_dropoff_datetime) AS drop_month,
    EXTRACT(YEAR FROM tpep_dropoff_datetime) AS drop_year,
    EXTRACT(DAYOFWEEK FROM tpep_dropoff_datetime) AS drop_weekday
FROM
    uber-de-project-raahul.Uber_de_bq.raw_table_2020;

-- Create fare_2020
CREATE TABLE uber-de-project-raahul.Uber_de_bq.fare_2020 AS
SELECT
    ROW_NUMBER() OVER () AS fare_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
FROM
    uber-de-project-raahul.Uber_de_bq.raw_table_2020
ORDER BY
    fare_id;

-- Create payment_2020
CREATE TABLE uber-de-project-raahul.Uber_de_bq.payment_2020 AS
SELECT
    ROW_NUMBER() OVER () AS payment_type_id,
    CASE
        WHEN payment_type = 1 THEN 'Credit card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No charge'
        WHEN payment_type = 4 THEN 'Dispute'
        WHEN payment_type = 5 THEN 'Unknown'
        WHEN payment_type = 6 THEN 'Voided trip'
    END AS payment_type_name,
    payment_type
FROM
    uber-de-project-raahul.Uber_de_bq.raw_table_2020
ORDER BY
    payment_type_id;

-- Create location_2020
CREATE TABLE uber-de-project-raahul.Uber_de_bq.location_2020 AS
SELECT
    ROW_NUMBER() OVER () AS location_id,
    PULocationID,
    DOLocationID
FROM
    uber-de-project-raahul.Uber_de_bq.raw_table_2020
ORDER BY
    location_id;

-- Create location_full_2020
CREATE TABLE uber-de-project-raahul.Uber_de_bq.location_full_2020 AS
SELECT
    l.location_id,
    l.PULocationID,
    l.DOLocationID,
    ll.Borough AS PULocation_Borough,
    ll.Zone AS PULocation_Zone,
    ll.service_zone AS PULocation_ServiceZone,
    ll2.Borough AS DOLocation_Borough,
    ll2.Zone AS DOLocation_Zone,
    ll2.service_zone AS DOLocation_ServiceZone
FROM
    uber-de-project-raahul.Uber_de_bq.location_2020 l
LEFT JOIN
    uber-de-project-raahul.Uber_de_bq.location_lookup ll ON l.PULocationID = ll.LocationID
LEFT JOIN
    uber-de-project-raahul.Uber_de_bq.location_lookup ll2 ON l.DOLocationID = ll2.LocationID
ORDER BY
    location_id;

-- Create passenger_count_2020
CREATE TABLE uber-de-project-raahul.Uber_de_bq.passenger_count_2020 AS
SELECT
    ROW_NUMBER() OVER () AS passenger_count_id,
    passenger_count
FROM
    uber-de-project-raahul.Uber_de_bq.raw_table_2020
ORDER BY
    passenger_count_id;

-- Create trip_distance_2020
CREATE TABLE uber-de-project-raahul.Uber_de_bq.trip_distance_2020 AS
SELECT
    ROW_NUMBER() OVER () AS trip_distance_id,
    trip_distance
FROM
    uber-de-project-raahul.Uber_de_bq.raw_table_2020
ORDER BY
    trip_distance_id;

-- Create rate_code_2020
CREATE TABLE uber-de-project-raahul.Uber_de_bq.rate_code_2020 AS
SELECT
    ROW_NUMBER() OVER () AS ratecode_id,
    CASE
        WHEN RatecodeID = 1 THEN 'Standard rate'
        WHEN RatecodeID = 2 THEN 'JFK'
        WHEN RatecodeID = 3 THEN 'Newark'
        WHEN RatecodeID = 4 THEN 'Nassau or Westchester'
        WHEN RatecodeID = 5 THEN 'Negotiated fare'
        WHEN RatecodeID = 6 THEN 'Group ride'
    END AS ratecode_name,
    RatecodeID
FROM
    uber-de-project-raahul.Uber_de_bq.raw_table_2020
ORDER BY
    ratecode_id;

-- Create analytics_2020
CREATE TABLE uber-de-project-raahul.Uber_de_bq.analytics_2020 AS
SELECT
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    d.pick_hour,
    d.pick_day,
    d.pick_month,
    d.pick_year,
    d.pick_weekday,
    d.drop_hour,
    d.drop_day,
    d.drop_month,
    d.drop_year,
    d.drop_weekday,
    fare.fare_amount,
    fare.tip_amount,
    fare.tolls_amount,
    fare.extra,
    fare.mta_tax,
    fare.improvement_surcharge,
    fare.total_amount,
    fare.congestion_surcharge,
    fare.airport_fee,
    p.passenger_count,
    pay.payment_type_name,
    rate.ratecode_name,
    loc.PULocation_Borough,
    loc.PULocation_Zone,
    loc.PULocation_ServiceZone,
    loc.DOLocation_Borough,
    loc.DOLocation_Zone,
    loc.DOLocation_ServiceZone,
    t.trip_distance
FROM
    uber-de-project-raahul.Uber_de_bq.raw_table_2020 AS r
JOIN
    uber-de-project-raahul.Uber_de_bq.datetime_table_2020 AS d ON r.main_id = d.datetime_id
JOIN
    uber-de-project-raahul.Uber_de_bq.fare_2020 AS fare ON r.main_id = fare.fare_id
JOIN
    uber-de-project-raahul.Uber_de_bq.location_full_2020 AS loc ON r.main_id = loc.location_id
JOIN
    uber-de-project-raahul.Uber_de_bq.passenger_count_2020 AS p ON r.main_id = p.passenger_count_id
JOIN
    uber-de-project-raahul.Uber_de_bq.payment_2020 AS pay ON r.main_id = pay.payment_type_id
JOIN
    uber-de-project-raahul.Uber_de_bq.rate_code_2020 AS rate ON r.main_id = rate.ratecode_id
JOIN
    uber-de-project-raahul.Uber_de_bq.trip_distance_2020 AS t ON r.main_id = t.trip_distance_id
ORDER BY
    tpep_pickup_datetime;
