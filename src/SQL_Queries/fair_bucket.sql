SELECT
  fare_bucket,
  pick_year,
  COUNT(*) AS count,
  ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM uber-de-project-raahul.Uber_de_bq.fair_combined), 2) AS percentage
FROM (
  SELECT
    total_amount,
    pick_year,
    CASE
      WHEN total_amount <= 10 THEN '0-10'
      WHEN total_amount <= 20 THEN '11-20'
      WHEN total_amount <= 30 THEN '21-30'
      ELSE 'Above 30'
    END AS fare_bucket
  FROM
  uber-de-project-raahul.Uber_de_bq.fair_combined
) AS subquery
GROUP BY pick_year, fare_bucket;
