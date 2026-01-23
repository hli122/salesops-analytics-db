-- :start_date, :end_date 例如 '2026-01-12' 到 '2026-01-18'
SELECT
  CAST(:start_date AS date) AS start_date,
  CAST(:end_date   AS date) AS end_date,
  ROUND(SUM(line_total), 2) AS revenue,
  SUM(units)                AS units,
  COUNT(*)                  AS line_count
FROM salesops.fact_sales_line
WHERE sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date);

SELECT
  s.seller_name,
  ROUND(SUM(f.line_total), 2) AS revenue,
  SUM(f.units)                AS units,
  COUNT(*)                    AS line_count
FROM salesops.fact_sales_line f
JOIN salesops.dim_seller s ON s.seller_id = f.seller_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
GROUP BY s.seller_name
ORDER BY revenue DESC;

SELECT
  p.product_code,
  ROUND(SUM(f.line_total), 2) AS revenue,
  SUM(f.units)                AS units
FROM salesops.fact_sales_line f
JOIN salesops.dim_product p ON p.product_id = f.product_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
GROUP BY p.product_code
ORDER BY revenue DESC
LIMIT 10;

SELECT
  COALESCE(sc.company_name, 'UNKNOWN') AS shipping_company,
  ROUND(SUM(f.line_total), 2) AS revenue,
  COUNT(*)                    AS line_count
FROM salesops.fact_sales_line f
LEFT JOIN salesops.dim_shipping_company sc
  ON sc.shipping_company_id = f.shipping_company_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
GROUP BY 1
ORDER BY revenue DESC;

