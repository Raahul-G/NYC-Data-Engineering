SELECT
  year,
  pickup_location,
  pickup_count,
FROM (
  SELECT
    '2020' AS year,
    PULocation_Zone_2020 AS pickup_location,
    COUNT(PULocation_Zone_2020) AS pickup_count,
  FROM uber-de-project-raahul.Uber_de_bq.analytics_2020
  GROUP BY PULocation_Zone_2020

  UNION ALL

  SELECT
    '2021' AS year,
    PULocation_Zone_2021 AS pickup_location,
    COUNT(PULocation_Zone_2021) AS pickup_count,
  FROM uber-de-project-raahul.Uber_de_bq.analytics_2021
  GROUP BY PULocation_Zone_2021
) AS subquery
ORDER BY year ASC;
