SELECT
  Hour, Year,
  COUNT(*) AS ride_count
FROM (
  SELECT
    DROP_Hour_2020 AS Hour, DROP_YEAR_2020 AS Year
  FROM uber-de-project-raahul.Uber_de_bq.analytics_2020
  WHERE DROP_YEAR_2020 = 2020
  UNION ALL
  SELECT
    DROP_Hour_2021 AS Hour, DROP_YEAR_2021 AS Year
  FROM uber-de-project-raahul.Uber_de_bq.analytics_2021
  WHERE DROP_YEAR_2021 = 2021
) AS subquery
GROUP BY Hour, Year
ORDER BY Hour, Year;