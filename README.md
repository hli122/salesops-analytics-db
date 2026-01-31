SalesOps Analytics DB

An internal Sales Operations analytics system with PostgreSQL, ETL pipelines, validation checks, and reporting APIs.

1️⃣ Project Overview

This project implements a Sales Operations analytics system designed for internal reporting and operational support.

It covers the full lifecycle of a data system:

Data ingestion (ETL from Excel)

Normalized relational database design

Data validation and quality checks

API-based reporting endpoints

Database reset and maintenance scripts

The system is intentionally designed to resemble enterprise / government internal tools, focusing on data correctness, maintainability, and auditability rather than UI.

2️⃣ Architecture
Excel (Sales Data)
        |
        v
ETL Script (Python / Pandas)
        |
        v
PostgreSQL (salesops schema)
        |
        v
FastAPI Reporting Layer
        |
        v
JSON APIs (Weekly Report / Rankings / Data Quality)

Key Design Principles

Single source of truth: PostgreSQL as the central data store

Separation of concerns:

ETL scripts

Database schema & maintenance

API / application layer

Configurable & environment-driven:

No hard-coded credentials

External data sources via environment variables

Audit-friendly:

Source file tracking

Source row numbers preserved

3️⃣ Project Structure
salesops-db/
│
├── app/                    # Application layer (FastAPI)
│   ├── __init__.py
│   ├── main.py
│   ├── db.py               # Centralized DB connection logic
│   └── routers/
│       ├── __init__.py
│       └── reports.py      # Reporting & data-quality APIs
│
├── scripts/
│   └── import_excel_to_pg.py   # ETL: Excel → PostgreSQL
│
├── sql/
│   ├── 01_create_tables.sql    # Schema & table definitions
│   └── 02_clean_tables.sql     # Database reset (truncate)
│
├── data/                   # Local data files (gitignored)
│
├── README.md
└── .gitignore

4️⃣ Database Design
Schema: salesops

Dimension tables

dim_product

dim_seller

dim_shipping_company

Fact table

fact_sales_line

Each sales line records:

Sale timestamp

Product, seller, shipping company (FKs)

Unit price, units, total price

source_file + source_row_number for audit & deduplication

5️⃣ ETL: Importing Sales Data
Data Source

Excel file (default):
data/2026SalesData.xlsx

Can be overridden via environment variable:

$env:SALES_DATA_FILE="data\Feb2026.xlsx"

Run ETL

From project root:

$env:DATABASE_URL="postgresql+psycopg2://user:password@localhost:5432/salesops"
python scripts\import_excel_to_pg.py

ETL Features

Column validation

Datetime parsing and validation

String normalization

Price sanity checks (unit_price × units vs total)

Idempotent inserts via (source_file, source_row_number)

Automatic dimension key creation

6️⃣ API Layer (FastAPI)

Start API server:

uvicorn app.main:app --reload


Swagger UI:

http://127.0.0.1:8000/docs

Available Endpoints
Endpoint	Description
/reports/weekly-summary	Revenue, units, line count
/reports/seller-ranking	Seller performance ranking
/reports/top-products	Top N products by revenue
/reports/shipping-breakdown	Shipping company revenue
/reports/data-quality	Data validation & anomaly detection
7️⃣ Data Quality & Validation

The data-quality API provides operational diagnostics:

Price mismatch detection (with tolerance)

Non-positive units

Negative monetary values

Missing shipping company references

Sample rows for troubleshooting

Example:

GET /reports/data-quality?start_date=2026-01-12&end_date=2026-01-18


Returns:

Overall status (ok / warn)

Aggregated issue counts

Example problematic rows

This mirrors real-world operational monitoring in enterprise systems.

8️⃣ Database Maintenance
Reset Database (Development / Testing)
-- sql/02_clean_tables.sql
TRUNCATE TABLE
  salesops.fact_sales_line,
  salesops.dim_product,
  salesops.dim_seller,
  salesops.dim_shipping_company
RESTART IDENTITY CASCADE;


This allows clean rebuilds when data sources or logic change.

9️⃣ Configuration
Variable	Purpose
DATABASE_URL	PostgreSQL connection string
SALES_DATA_FILE	Optional override for Excel data file

No credentials or data files are committed to GitHub.

🔟 Intended Use Case

This project is designed as:

An internal Sales Operations analytics backend

A reference implementation of:

ETL pipelines

Data validation

Reporting APIs

Maintainable system structure

It intentionally avoids UI complexity to focus on data correctness, reliability, and system design.

🚀 Future Improvements

Import logging table (import_log)

Scheduled ETL runs

Authentication / role-based access

Materialized views for performance

CSV / Excel export endpoints

📌 Summary

This repository demonstrates the design and implementation of a production-style internal analytics system, emphasizing:

Clean architecture

Operational reliability

Data quality awareness

Realistic enterprise workflows