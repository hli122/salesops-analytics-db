SQL_DATA_QUALITY_SUMMARY = """
WITH base AS (
  SELECT
    line_id,
    sale_time,
    sale_date,
    unit_price,
    units,
    line_total,
    shipping_company_id,
    ROUND((unit_price * units)::numeric, 2) AS expected_total
  FROM salesops.fact_sales_line
  WHERE sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
)
SELECT
  CAST(:start_date AS date) AS start_date,
  CAST(:end_date   AS date) AS end_date,

  COUNT(*) AS rows_in_range,

  SUM(CASE WHEN ABS(line_total - expected_total) > CAST(:tol AS numeric)
           THEN 1 ELSE 0 END) AS mismatched_total_count,

  SUM(CASE WHEN units <= 0 THEN 1 ELSE 0 END) AS nonpositive_units_count,

  SUM(CASE WHEN unit_price < 0 OR line_total < 0 THEN 1 ELSE 0 END) AS negative_amount_count,

  SUM(CASE WHEN shipping_company_id IS NULL THEN 1 ELSE 0 END) AS missing_shipping_company_count

FROM base;
"""

SQL_DATA_QUALITY_SAMPLES = """
SELECT
  f.line_id,
  f.sale_time,
  f.sale_date,
  p.product_code,
  s.seller_name,
  COALESCE(sc.company_name, 'UNKNOWN') AS shipping_company,
  f.unit_price,
  f.units,
  f.line_total,
  ROUND((f.unit_price * f.units)::numeric, 2) AS expected_total,
  ROUND((f.line_total - ROUND((f.unit_price * f.units)::numeric, 2))::numeric, 2) AS diff
FROM salesops.fact_sales_line f
JOIN salesops.dim_product p ON p.product_id = f.product_id
JOIN salesops.dim_seller s ON s.seller_id = f.seller_id
LEFT JOIN salesops.dim_shipping_company sc ON sc.shipping_company_id = f.shipping_company_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
  AND (
    ABS(f.line_total - ROUND((f.unit_price * f.units)::numeric, 2)) > CAST(:tol AS numeric)
    OR f.units <= 0
    OR f.unit_price < 0
    OR f.line_total < 0
  )
ORDER BY f.sale_time
LIMIT :limit;
"""
