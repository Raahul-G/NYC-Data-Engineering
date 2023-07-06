SELECT
  Month, Year,
  COUNT(*) AS ride_count
FROM (
  SELECT
    DROP_MONTH_2020 AS Month, DROP_YEAR_2020 AS Year
  FROM uber-de-project-raahul.Uber_de_bq.analytics_2020
  WHERE DROP_YEAR_2020 = 2020
  UNION ALL
  SELECT
    DROP_MONTH_2021 AS Month, DROP_YEAR_2021 AS Year
  FROM uber-de-project-raahul.Uber_de_bq.analytics_2021
  WHERE DROP_YEAR_2021 = 2021
) AS subquery
GROUP BY Month, Year
ORDER BY Month, Year;
