from fastapi import APIRouter, Query
from sqlalchemy import text
from app.db import get_engine
from app.queries.report_queries import (
    SQL_WEEKLY_SUMMARY,
    SQL_SELLER_RANKING,
    SQL_TOP_PRODUCTS,
    SQL_SHIPPING,
)
from app.queries.dq_queries import (
    SQL_DATA_QUALITY_SUMMARY,
    SQL_DATA_QUALITY_SAMPLES
)
router = APIRouter(prefix="/reports", tags=["reports"])


# =========================
# Weekly Summary
# =========================

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

@router.get("/data-quality-samples")
def data_quality_samples(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    tol: float = Query(0.05, ge=0, description="Tolerance for total mismatch"),
    limit: int = Query(20, ge=1, le=200),
):
    engine = get_engine()
    params = {"start_date": start_date, "end_date": end_date, "tol": tol, "limit": limit}

    with engine.connect() as conn:
        rows = conn.execute(text(SQL_DATA_QUALITY_SAMPLES), params).mappings().all()

    # 把 datetime/date 转成字符串，避免 JSON 序列化问题
    samples = []
    for r in rows:
        d = dict(r)
        if d.get("sale_time") is not None:
            d["sale_time"] = d["sale_time"].isoformat()
        if d.get("sale_date") is not None:
            d["sale_date"] = d["sale_date"].isoformat()
        samples.append(d)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "tol": tol,
        "limit": limit,
        "count": len(samples),
        "samples": samples,
    }