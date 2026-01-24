from fastapi import APIRouter, Query
from sqlalchemy import text
from app.db import get_engine

router = APIRouter(prefix="/reports", tags=["reports"])

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
    end_date: str = Query(..., description="YYYY-MM-DD")
):
    engine = get_engine()
    params = {"start_date": start_date, "end_date": end_date}

    with engine.connect() as conn:
        row = conn.execute(text(SQL_WEEKLY_SUMMARY), params).mappings().first()

    # row is a dict-like mapping
    return dict(row) if row else {
        "start_date": start_date,
        "end_date": end_date,
        "revenue": 0,
        "units": 0,
        "line_count": 0
    }

SQL_SELLER_RANKING = """
SELECT
  s.seller_name,
  ROUND(COALESCE(SUM(f.line_total), 0), 2) AS revenue,
  ROUND(COALESCE(SUM(f.units), 0), 2)      AS units,
  COUNT(*)                    AS line_count
FROM salesops.fact_sales_line f
JOIN salesops.dim_seller s ON s.seller_id = f.seller_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
GROUP BY s.seller_name
ORDER BY revenue DESC;
"""

@router.get("/seller-ranking")
def seller_ranking(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD")
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
    limit: int = Query(10, ge=1, le=50)  # 默认10，最少1，最多50
):
    engine = get_engine()
    params = {"start_date": start_date, "end_date": end_date, "limit": limit}

    with engine.connect() as conn:
        rows = conn.execute(text(SQL_TOP_PRODUCTS), params).mappings().all()

    return {
        "start_date": start_date,
        "end_date": end_date,
        "top_products": list(rows)
    }

SQL_SHIPPING = """
SELECT
  COALESCE(sc.company_name, 'UNKNOWN') AS shipping_company,
  ROUND(COALESCE(SUM(f.line_total), 0), 2) AS revenue,
  COUNT(*)                    AS line_count
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
    end_date: str = Query(..., description="YYYY-MM-DD")
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
