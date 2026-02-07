import os
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, datetime
import argparse

# Ensure project root is on sys.path so "import app" works
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from app.db import get_engine

engine = get_engine()

def parse_args():
    parser = argparse.ArgumentParser(description="Generate weekly sales report")
    parser.add_argument(
        "--start",
        dest="start_date",
        default="2026-01-19",
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end",
        dest="end_date",
        default="2026-01-23",
        help="End date (YYYY-MM-DD)"
    )
    return parser.parse_args()

SQL_SUMMARY = """
SELECT
  CAST(:start_date AS date) AS start_date,
  CAST(:end_date   AS date) AS end_date,
  ROUND(COALESCE(SUM(line_total), 0), 2) AS revenue,
  ROUND(COALESCE(SUM(units), 0), 2)      AS units,
  COUNT(*)                  AS line_count
FROM salesops.fact_sales_line
WHERE sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date);
"""

SQL_SELLER = """
SELECT
  s.seller_name,
  ROUND(COALESCE(SUM(line_total), 0), 2) AS revenue,
  ROUND(COALESCE(SUM(units), 0), 2)      AS units,
  COUNT(*)                    AS line_count
FROM salesops.fact_sales_line f
JOIN salesops.dim_seller s ON s.seller_id = f.seller_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
GROUP BY s.seller_name
ORDER BY revenue DESC;
"""

SQL_TOP_PRODUCTS = """
SELECT
  p.product_code,
  ROUND(COALESCE(SUM(line_total), 0), 2) AS revenue,
  ROUND(COALESCE(SUM(units), 0), 2)      AS units
FROM salesops.fact_sales_line f
JOIN salesops.dim_product p ON p.product_id = f.product_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
GROUP BY p.product_code
ORDER BY revenue DESC
LIMIT 10;
"""

SQL_SHIPPING = """
SELECT
  COALESCE(sc.company_name, 'UNKNOWN') AS shipping_company,
  ROUND(COALESCE(SUM(line_total), 0), 2) AS revenue,
  COUNT(*)                    AS line_count
FROM salesops.fact_sales_line f
LEFT JOIN salesops.dim_shipping_company sc
  ON sc.shipping_company_id = f.shipping_company_id
WHERE f.sale_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
GROUP BY 1
ORDER BY revenue DESC;
"""

def df_query(sql: str, params: dict) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)

def main():
    args = parse_args()
    START_DATE = args.start_date
    END_DATE = args.end_date
    params = {"start_date": START_DATE, "end_date": END_DATE}

    df_summary = df_query(SQL_SUMMARY, params)
    df_seller = df_query(SQL_SELLER, params)
    df_top = df_query(SQL_TOP_PRODUCTS, params)
    df_ship = df_query(SQL_SHIPPING, params)

    # 1) Export an Excel weekly report
    os.makedirs("outputs", exist_ok=True)
    out_xlsx = os.path.join("outputs", f"weekly_report_{START_DATE}_to_{END_DATE}.xlsx")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df_summary.to_excel(writer, index=False, sheet_name="Summary")
        df_seller.to_excel(writer, index=False, sheet_name="Seller_Ranking")
        df_top.to_excel(writer, index=False, sheet_name="Top_Products")
        df_ship.to_excel(writer, index=False, sheet_name="Shipping_Share")

    # 2) Build a boss-friendly text (copy-paste into WeChat)
    s = df_summary.iloc[0]
    revenue = float(s["revenue"] or 0)
    units = float(s["units"] or 0)
    line_count = int(s["line_count"] or 0)

    top_seller_line = ""
    if len(df_seller) > 0:
        ts = df_seller.iloc[0]
        top_seller_line = f"- Top Seller: {ts['seller_name']}  Revenue ${float(ts['revenue']):.2f}\n"

    top_product_line = ""
    if len(df_top) > 0:
        tp = df_top.iloc[0]
        top_product_line = f"- Top Product: {tp['product_code']}  Revenue ${float(tp['revenue']):.2f} (Units {float(tp['units']):.0f})\n"

    msg = (
        f"Weekly Sales Report ({START_DATE} ~ {END_DATE})\n"
        f"- Total Revenue: ${revenue:.2f}\n"
        f"- Total Units: {units:.0f}\n"
        f"- Line Items: {line_count}\n"
        f"{top_seller_line}"
        f"{top_product_line}"
        f"\n(Details attached: {out_xlsx})"
    )

    print(msg)
    print(f"\nSaved Excel report: {out_xlsx} ✅")

if __name__ == "__main__":
    main()
