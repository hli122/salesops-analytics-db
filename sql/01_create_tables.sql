-- =========================
-- Schema
-- =========================
CREATE SCHEMA IF NOT EXISTS salesops;

-- =========================
-- Dimension tables
-- =========================

CREATE TABLE IF NOT EXISTS salesops.dim_product (
  product_id      BIGSERIAL PRIMARY KEY,
  product_code    TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_dim_product_code UNIQUE (product_code),
  CONSTRAINT chk_product_code_nonempty CHECK (length(btrim(product_code)) > 0)
);

CREATE TABLE IF NOT EXISTS salesops.dim_seller (
  seller_id       BIGSERIAL PRIMARY KEY,
  seller_name     TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_dim_seller_name UNIQUE (seller_name),
  CONSTRAINT chk_seller_name_nonempty CHECK (length(btrim(seller_name)) > 0)
);

CREATE TABLE IF NOT EXISTS salesops.dim_shipping_company (
  shipping_company_id BIGSERIAL PRIMARY KEY,
  company_name        TEXT NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_dim_ship_company_name UNIQUE (company_name),
  CONSTRAINT chk_ship_company_name_nonempty CHECK (length(btrim(company_name)) > 0)
);

-- =========================
-- Fact table: one row per excel line item
-- =========================

CREATE TABLE IF NOT EXISTS salesops.fact_sales_line (
  line_id             BIGSERIAL PRIMARY KEY,

  sale_time           TIMESTAMPTZ NOT NULL,
  sale_date           DATE GENERATED ALWAYS AS ((sale_time AT TIME ZONE 'UTC')::date) STORED,

  product_id          BIGINT NOT NULL REFERENCES salesops.dim_product(product_id),
  seller_id           BIGINT NOT NULL REFERENCES salesops.dim_seller(seller_id),
  shipping_company_id BIGINT NULL REFERENCES salesops.dim_shipping_company(shipping_company_id),

  unit_price          NUMERIC(12,2) NOT NULL,
  units               NUMERIC(12,2) NOT NULL,
  line_total          NUMERIC(12,2) NOT NULL,

  source_file         TEXT NOT NULL,
  source_row_number   INTEGER NOT NULL,
  ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Basic sanity constraints
  CONSTRAINT chk_unit_price_nonneg CHECK (unit_price >= 0),
  CONSTRAINT chk_units_pos CHECK (units > 0),
  CONSTRAINT chk_line_total_nonneg CHECK (line_total >= 0),

  -- Prevent duplicate re-import for same file & row
  CONSTRAINT uq_fact_source UNIQUE (source_file, source_row_number)
);

-- Helpful indexes for reporting
CREATE INDEX IF NOT EXISTS idx_fact_sale_date ON salesops.fact_sales_line (sale_date);
CREATE INDEX IF NOT EXISTS idx_fact_seller_date ON salesops.fact_sales_line (seller_id, sale_date);
CREATE INDEX IF NOT EXISTS idx_fact_product_date ON salesops.fact_sales_line (product_id, sale_date);
CREATE INDEX IF NOT EXISTS idx_fact_ship_date ON salesops.fact_sales_line (shipping_company_id, sale_date);

-- =========================
-- Optional views for daily/weekly reports
-- =========================

CREATE OR REPLACE VIEW salesops.vw_daily_sales AS
SELECT
  sale_date,
  ROUND(SUM(line_total), 2) AS revenue,
  SUM(units)                AS units,
  COUNT(*)                  AS line_count
FROM salesops.fact_sales_line
GROUP BY sale_date
ORDER BY sale_date;

CREATE OR REPLACE VIEW salesops.vw_weekly_sales AS
SELECT
  date_trunc('week', sale_date::timestamp)::date AS week_start_monday,
  ROUND(SUM(line_total), 2) AS revenue,
  SUM(units)                AS units,
  COUNT(*)                  AS line_count
FROM salesops.fact_sales_line
GROUP BY 1
ORDER BY 1;

