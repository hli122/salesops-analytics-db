# Weekly summary
SQL_WEEKLY_SUMMARY = """
SELECT
  CAST(:start_date AS date) AS start_date,
  CAST(:end_date   AS date) AS end_date,
  ROUND(COALESCE(SUM(line_total), 0), 2) AS revenue,
  ROUND(COALESCE(SUM(units), 0), 2)      AS units,
  COUNT(*)                               AS line_count
FROM salesops.fact_sales_line
WHERE sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date);
"""


SQL_SELLER_RANKING = """
SELECT
  s.seller_name,
  ROUND(COALESCE(SUM(f.line_total), 0), 2) AS revenue,
  ROUND(COALESCE(SUM(f.units), 0), 2)      AS units,
  COUNT(*)                                  AS line_count
FROM salesops.fact_sales_line f
JOIN salesops.dim_seller s ON s.seller_id = f.seller_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
GROUP BY s.seller_name
ORDER BY revenue DESC;
"""


SQL_TOP_PRODUCTS = """
SELECT
  p.product_code,
  ROUND(COALESCE(SUM(f.line_total), 0), 2) AS revenue,
  ROUND(COALESCE(SUM(f.units), 0), 2)      AS units
FROM salesops.fact_sales_line f
JOIN salesops.dim_product p ON p.product_id = f.product_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
GROUP BY p.product_code
ORDER BY revenue DESC
LIMIT :limit;
"""


SQL_SHIPPING = """
SELECT
  COALESCE(sc.company_name, 'UNKNOWN') AS shipping_company,
  ROUND(COALESCE(SUM(f.line_total), 0), 2) AS revenue,
  COUNT(*)                                  AS line_count
FROM salesops.fact_sales_line f
LEFT JOIN salesops.dim_shipping_company sc
  ON sc.shipping_company_id = f.shipping_company_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
GROUP BY 1
ORDER BY revenue DESC;
"""
