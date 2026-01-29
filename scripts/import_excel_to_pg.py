import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


# ====== config ======
DEFAULT_EXCEL_PATH = Path("data") / "2026SalesData.xlsx"
EXCEL_PATH = Path(os.getenv("SALES_DATA_FILE", str(DEFAULT_EXCEL_PATH)))

if not EXCEL_PATH.exists():
    raise FileNotFoundError(
        f"Excel file not found: {EXCEL_PATH}\n"
        "Tip: Put your file under ./data/ or set env var SALES_DATA_FILE.\n"
        'PowerShell example: $env:SALES_DATA_FILE="data\\2026SalesData.xlsx"'
    )

SOURCE_FILE_NAME = EXCEL_PATH.name

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "Missing env DATABASE_URL.\n"
        "PowerShell example:\n"
        '$env:DATABASE_URL="postgresql+psycopg2://postgres:YOUR_PASSWORD@localhost:5432/salesops"'
    )

engine = create_engine(DATABASE_URL, future=True)


# ====== helpers ======
def norm_str(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s else None


def approx_equal(a, b, tol=0.01):
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


def get_or_create_product_id(conn, product_code: str) -> int:
    row = conn.execute(
        text("SELECT product_id FROM salesops.dim_product WHERE product_code = :v"),
        {"v": product_code},
    ).fetchone()
    if row:
        return int(row.product_id)

    return int(
        conn.execute(
            text("INSERT INTO salesops.dim_product (product_code) VALUES (:v) RETURNING product_id"),
            {"v": product_code},
        ).fetchone().product_id
    )


def get_or_create_seller_id(conn, seller_name: str) -> int:
    row = conn.execute(
        text("SELECT seller_id FROM salesops.dim_seller WHERE seller_name = :v"),
        {"v": seller_name},
    ).fetchone()
    if row:
        return int(row.seller_id)

    return int(
        conn.execute(
            text("INSERT INTO salesops.dim_seller (seller_name) VALUES (:v) RETURNING seller_id"),
            {"v": seller_name},
        ).fetchone().seller_id
    )


def get_or_create_shipping_id(conn, company_name: str) -> int:
    row = conn.execute(
        text("SELECT shipping_company_id FROM salesops.dim_shipping_company WHERE company_name = :v"),
        {"v": company_name},
    ).fetchone()
    if row:
        return int(row.shipping_company_id)

    return int(
        conn.execute(
            text("INSERT INTO salesops.dim_shipping_company (company_name) VALUES (:v) RETURNING shipping_company_id"),
            {"v": company_name},
        ).fetchone().shipping_company_id
    )


# ====== main ======
def main():
    # Read excel
    df = pd.read_excel(str(EXCEL_PATH))

    expected = ["Time", "Product", "Seller", "Unit Price", "Units", "Total Price", "Shipping Company"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns in Excel: {missing}. Found: {list(df.columns)}")

    # Keep original excel row number (best effort: header is row 1; data starts row 2)
    df["_source_row_number"] = df.index + 2

    # Parse datetime
    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
    if df["Time"].isna().any():
        bad = df[df["Time"].isna()][["_source_row_number", "Time", "Product", "Seller"]].head(20)
        raise RuntimeError(f"Found unparsable Time values (showing up to 20):\n{bad}")

    # Clean strings
    df["Product"] = df["Product"].apply(norm_str)
    df["Seller"] = df["Seller"].apply(norm_str)
    df["Shipping Company"] = df["Shipping Company"].apply(norm_str)

    # Drop rows missing required fields
    df = df.dropna(subset=["Product", "Seller", "Unit Price", "Units", "Total Price"]).copy()

    # Price math sanity check (warn only)
    for _, r in df.iterrows():
        calc = float(r["Unit Price"]) * float(r["Units"])
        if not approx_equal(calc, r["Total Price"], tol=0.05):
            print(
                f"[WARN] Excel row {int(r['_source_row_number'])}: "
                f"unit_price*units={calc:.2f}, total={float(r['Total Price']):.2f}"
            )

    inserted = 0
    skipped = 0

    with engine.begin() as conn:
        for _, r in df.iterrows():
            source_row_number = int(r["_source_row_number"])

            product_id = get_or_create_product_id(conn, r["Product"])
            seller_id = get_or_create_seller_id(conn, r["Seller"])

            shipping_company_id = None
            if r["Shipping Company"]:
                shipping_company_id = get_or_create_shipping_id(conn, r["Shipping Company"])

            res = conn.execute(
                text(
                    """
                    INSERT INTO salesops.fact_sales_line
                      (sale_time, product_id, seller_id, shipping_company_id,
                       unit_price, units, line_total, source_file, source_row_number)
                    VALUES
                      (:sale_time, :product_id, :seller_id, :shipping_company_id,
                       :unit_price, :units, :line_total, :source_file, :source_row_number)
                    ON CONFLICT (source_file, source_row_number) DO NOTHING
                    """
                ),
                {
                    "sale_time": r["Time"].to_pydatetime(),
                    "product_id": product_id,
                    "seller_id": seller_id,
                    "shipping_company_id": shipping_company_id,
                    "unit_price": round(float(r["Unit Price"]), 2),
                    "units": round(float(r["Units"]), 2),
                    "line_total": round(float(r["Total Price"]), 2),
                    "source_file": SOURCE_FILE_NAME,
                    "source_row_number": source_row_number,
                },
            )

            if res.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

    print(f"Import complete ✅ Inserted={inserted}, Skipped(duplicate)={skipped}")
    print(f"Source file: {SOURCE_FILE_NAME}")


if __name__ == "__main__":
    main()
