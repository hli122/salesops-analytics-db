from fastapi import APIRouter, Query
from sqlalchemy import text
from app.db import get_engine

router = APIRouter(prefix="/reports", tags=["reports"])


# =========================
# Weekly Summary
# =========================
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

@router.get("/weekly-summary")
def weekly_summary(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
):
    engine = get_engine()
    params = {"start_date": start_date, "end_date": end_date}

    with engine.connect() as conn:
        row = conn.execute(text(SQL_WEEKLY_SUMMARY), params).mappings().first()

    return dict(row) if row else {
        "start_date": start_date,
        "end_date": end_date,
        "revenue": 0,
        "units": 0,
        "line_count": 0
    }


# =========================
# Seller Ranking
# =========================
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

@router.get("/seller-ranking")
def seller_ranking(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
):
    engine = get_engine()
    params = {"start_date": start_date, "end_date": end_date}

    with engine.connect() as conn:
        rows = conn.execute(text(SQL_SELLER_RANKING), params).mappings().all()

    return {
        "start_date": start_date,
        "end_date": end_date,
        "sellers": list(rows)
    }


# =========================
# Top Products
# =========================
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

@router.get("/top-products")
def top_products(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(12, ge=1, le=50, description="Max number of products"),
):
    engine = get_engine()
    params = {"start_date": start_date, "end_date": end_date, "limit": limit}

    with engine.connect() as conn:
        rows = conn.execute(text(SQL_TOP_PRODUCTS), params).mappings().all()

    return {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
        "top_products": list(rows)
    }


# =========================
# Shipping Breakdown
# =========================
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

@router.get("/shipping-breakdown")
def shipping_breakdown(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
):
    engine = get_engine()
    params = {"start_date": start_date, "end_date": end_date}

    with engine.connect() as conn:
        rows = conn.execute(text(SQL_SHIPPING), params).mappings().all()

    return {
        "start_date": start_date,
        "end_date": end_date,
        "shipping_companies": list(rows)
    }


# =========================
# Data Quality (Validation + Troubleshooting)
# =========================
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

@router.get("/data-quality")
def data_quality(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    tol: float = Query(0.05, ge=0.0, le=5.0, description="Mismatch tolerance in dollars"),
    limit: int = Query(20, ge=1, le=200, description="Max sample rows returned"),
):
    engine = get_engine()
    params = {"start_date": start_date, "end_date": end_date, "tol": tol}

    with engine.connect() as conn:
        summary = conn.execute(text(SQL_DATA_QUALITY_SUMMARY), params).mappings().first()
        samples = conn.execute(
            text(SQL_DATA_QUALITY_SAMPLES),
            {**params, "limit": limit}
        ).mappings().all()

    status = "ok"
    if summary:
        if (
            summary["mismatched_total_count"] > 0
            or summary["nonpositive_units_count"] > 0
            or summary["negative_amount_count"] > 0
        ):
            status = "warn"

    return {
        "status": status,
        "summary": dict(summary) if summary else None,
        "samples": list(samples),
        "notes": {
            "mismatch_rule": f"abs(line_total - round(unit_price*units,2)) > {tol}",
            "sample_limit": limit,
        },
    }
