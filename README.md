SalesOps Analytics Database & ETL Pipeline

A lightweight Sales Operations analytics system built with PostgreSQL + Python, featuring a full ETL pipeline to ingest Excel-based sales data, a dimensional data model, and an automated weekly reporting workflow.

This project demonstrates how to transform raw operational spreadsheets into a structured analytics database and generate management-ready reports.

I. Features

ETL Pipeline (Extract · Transform · Load)

Import sales data from Excel into PostgreSQL

Data cleaning and normalization

Automatic deduplication and integrity constraints

Dimensional Data Model

Fact table: fact_sales_line

Dimension tables: dim_product, dim_seller, dim_shipping_company

Optimized for reporting and aggregation

Automated Weekly Reporting

Revenue, units sold, and transaction volume

Seller performance ranking

Top products by revenue

Shipping company breakdown

Production-style Engineering Practices

Environment variable based configuration

Idempotent imports

Indexes and constraints

Git-ignored secrets and generated files

II. Architecture Overview
Excel Files
    ↓
Python ETL (pandas + SQLAlchemy)
    ↓
PostgreSQL (SalesOps schema)
    ↓
Analytical SQL Queries
    ↓
Weekly Management Report

III. Project Structure
salesops-analytics-db/
├── scripts/
│   ├── import_excel_to_pg.py      # ETL pipeline: Excel → PostgreSQL
│   └── generate_weekly_report.py  # Weekly analytics & reporting
│
├── sql/
│   ├── 01_create_tables.sql       # Schema & tables
│   └── 02_clean_tables.sql        # Reset data (truncate)
│
├── data/
│   └── (ignored) real business Excel files
│
├── .gitignore
└── README.md

IV. Data Model

Fact table

salesops.fact_sales_line

sale_time, product_id, seller_id, shipping_company_id

unit_price, units, line_total

source tracking & ingestion timestamp

Dimensions

salesops.dim_product

salesops.dim_seller

salesops.dim_shipping_company

The schema follows a star-schema style design optimized for analytics workloads.

V. Setup
1. Install dependencies
pip install pandas openpyxl sqlalchemy psycopg2-binary

2. Create database & tables

Create a PostgreSQL database (e.g. salesops), then run:

-- in pgAdmin
sql/01_create_tables.sql

3. Configure environment variables

Windows PowerShell:

$env:DATABASE_URL="postgresql+psycopg2://username:password@localhost:5432/salesops"

4. Run ETL import
python scripts/import_excel_to_pg.py

5. Generate weekly report
$env:START_DATE="2026-01-12"
$env:END_DATE="2026-01-18"
python scripts/generate_weekly_report.py

VI. Example Analytics

Weekly revenue, total units, order volume

Seller ranking by revenue

Top products

Shipping company contribution

All analytics are computed directly from the PostgreSQL warehouse.

VII. Security & Data Handling

Database credentials are loaded from environment variables

Real business Excel files and generated reports are excluded via .gitignore

Repository only contains reproducible system code

VIII. Tech Stack

PostgreSQL – analytics database

Python – ETL & reporting

pandas – data processing

SQLAlchemy – database access

openpyxl – Excel ingestion

IX. Project Goals

Build a realistic sales analytics backend system

Demonstrate ETL engineering practices

Provide a foundation for dashboards, APIs, or scheduled reporting jobs

X. Possible Extensions

Scheduled jobs (cron / Task Scheduler)

Web dashboard (FastAPI + React)

Automated email reports

Data quality checks & anomaly detection

Visualization layer (Metabase / Superset / Power BI)


Author

Haoran Li
Sales Operations · Data Engineering · Analytics Systems