-- =============================================================================
-- create_tables.sql
-- Amazon Redshift DDL — Orders ETL Pipeline
--
-- Tables:
--   orders.fact_orders          Primary fact table (EVEN distribution, compound sort)
--   orders.fact_orders_staging  Staging table for COPY + UPSERT pattern
--   orders.dim_product          Product dimension (KEY distribution on product_id)
--   orders.dim_customer         Customer dimension (KEY distribution on customer_id)
--   orders.dim_date             Date dimension (ALL distribution)
--   orders.etl_audit_log        ETL run metadata and row counts
--
-- Design principles:
--   - Distribution: EVEN for large fact tables, KEY for dimension lookups,
--                   ALL for small dimensions (< 1M rows)
--   - Sort keys:    Compound sort on the most frequent filter columns
--                   (order_date first — time-range queries dominate)
--   - Compression:  ENCODE AZ64 for numerics, ZSTD for strings (Redshift default)
--   - Constraints:  NOT NULL only — Redshift does not enforce FK constraints,
--                   but they inform the query optimizer
-- =============================================================================

-- =============================================================================
-- 0. Schema
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS orders;

-- =============================================================================
-- 1. Dimension Tables
-- =============================================================================

-- ---------------------------------------------------------------------------
-- dim_date — Date dimension (ALL distribution — small table, fast joins)
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS orders.dim_date CASCADE;

CREATE TABLE orders.dim_date
(
    date_sk                 INTEGER         NOT NULL ENCODE AZ64,
    full_date               DATE            NOT NULL ENCODE AZ64,
    calendar_year           SMALLINT        NOT NULL ENCODE AZ64,
    calendar_quarter        SMALLINT        NOT NULL ENCODE AZ64,
    calendar_month          SMALLINT        NOT NULL ENCODE AZ64,
    month_name              VARCHAR(10)     NOT NULL ENCODE ZSTD,
    month_name_short        VARCHAR(3)      NOT NULL ENCODE ZSTD,
    week_of_year            SMALLINT        NOT NULL ENCODE AZ64,
    day_of_year             SMALLINT        NOT NULL ENCODE AZ64,
    day_of_month            SMALLINT        NOT NULL ENCODE AZ64,
    day_of_week             SMALLINT        NOT NULL ENCODE AZ64,
    day_name                VARCHAR(10)     NOT NULL ENCODE ZSTD,
    day_name_short          VARCHAR(3)      NOT NULL ENCODE ZSTD,
    is_weekend              BOOLEAN         NOT NULL ENCODE RAW,
    is_weekday              BOOLEAN         NOT NULL ENCODE RAW,
    fiscal_year             SMALLINT        NOT NULL ENCODE AZ64,
    fiscal_quarter          SMALLINT        NOT NULL ENCODE AZ64,
    fiscal_month            SMALLINT        NOT NULL ENCODE AZ64,
    is_current_month        BOOLEAN         NOT NULL ENCODE RAW,
    is_current_year         BOOLEAN         NOT NULL ENCODE RAW,
    year_month              VARCHAR(7)      NOT NULL ENCODE ZSTD,
    quarter_label           VARCHAR(10)     NOT NULL ENCODE ZSTD,
    fiscal_quarter_label    VARCHAR(12)     NOT NULL ENCODE ZSTD
)
DISTSTYLE ALL
SORTKEY (full_date);

COMMENT ON TABLE orders.dim_date IS
    'Conformed date dimension. Covers 1990-01-01 to 2040-12-31. '
    'DISTSTYLE ALL ensures broadcast-join efficiency with all fact tables.';


-- ---------------------------------------------------------------------------
-- dim_customer — Customer dimension (KEY distribution for fact join)
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS orders.dim_customer CASCADE;

CREATE TABLE orders.dim_customer
(
    customer_sk             BIGINT          NOT NULL ENCODE AZ64,
    customer_id             VARCHAR(50)     NOT NULL ENCODE ZSTD,
    customer_name           VARCHAR(200)    NOT NULL ENCODE ZSTD,
    email                   VARCHAR(254)    ENCODE ZSTD,
    customer_segment        VARCHAR(50)     ENCODE ZSTD,
    account_tier            VARCHAR(20)     ENCODE ZSTD,
    region                  VARCHAR(50)     ENCODE ZSTD,
    country                 VARCHAR(100)    ENCODE ZSTD,
    city                    VARCHAR(100)    ENCODE ZSTD,
    postal_code             VARCHAR(20)     ENCODE ZSTD,
    registered_date         DATE            ENCODE AZ64,
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE ENCODE RAW,
    -- SCD2 metadata
    effective_from          DATE            NOT NULL ENCODE AZ64,
    effective_to            DATE            NOT NULL ENCODE AZ64,
    is_current              BOOLEAN         NOT NULL ENCODE RAW,
    _dbt_scd_id             CHAR(32)        ENCODE ZSTD,
    -- Audit
    _loaded_at              TIMESTAMP       NOT NULL DEFAULT GETDATE() ENCODE AZ64
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (customer_id, effective_from);

COMMENT ON TABLE orders.dim_customer IS
    'Customer dimension with SCD Type 2 history. '
    'Distributed by customer_id to co-locate with fact_orders join.';


-- ---------------------------------------------------------------------------
-- dim_product — Product dimension (KEY distribution for fact join)
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS orders.dim_product CASCADE;

CREATE TABLE orders.dim_product
(
    product_sk              BIGINT          NOT NULL ENCODE AZ64,
    product_id              VARCHAR(50)     NOT NULL ENCODE ZSTD,
    product_name            VARCHAR(500)    NOT NULL ENCODE ZSTD,
    product_category        VARCHAR(100)    ENCODE ZSTD,
    product_sub_category    VARCHAR(100)    ENCODE ZSTD,
    brand                   VARCHAR(100)    ENCODE ZSTD,
    manufacturer            VARCHAR(100)    ENCODE ZSTD,
    unit_cost               NUMERIC(12, 2)  ENCODE AZ64,
    retail_price            NUMERIC(12, 2)  ENCODE AZ64,
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE ENCODE RAW,
    -- Audit
    _loaded_at              TIMESTAMP       NOT NULL DEFAULT GETDATE() ENCODE AZ64
)
DISTSTYLE KEY
DISTKEY (product_id)
COMPOUND SORTKEY (product_category, product_id);

COMMENT ON TABLE orders.dim_product IS
    'Product catalogue dimension. Distributed by product_id to '
    'co-locate with fact_orders for efficient hash joins.';


-- =============================================================================
-- 2. Fact Tables
-- =============================================================================

-- ---------------------------------------------------------------------------
-- fact_orders — Transaction fact at order line item grain
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS orders.fact_orders CASCADE;

CREATE TABLE orders.fact_orders
(
    -- ── Surrogate key ────────────────────────────────────────────────────────
    order_line_sk           CHAR(32)        NOT NULL ENCODE ZSTD,

    -- ── Foreign keys ─────────────────────────────────────────────────────────
    customer_sk             BIGINT          NOT NULL DEFAULT -1 ENCODE AZ64,
    product_sk              BIGINT          NOT NULL DEFAULT -1 ENCODE AZ64,
    order_date_sk           INTEGER         NOT NULL DEFAULT -1 ENCODE AZ64,
    ship_date_sk            INTEGER                           ENCODE AZ64,

    -- ── Natural keys (degenerate dimensions) ─────────────────────────────────
    order_id                VARCHAR(100)    NOT NULL ENCODE ZSTD,
    customer_id             VARCHAR(50)     NOT NULL ENCODE ZSTD,
    product_id              VARCHAR(50)              ENCODE ZSTD,
    sales_rep_id            VARCHAR(50)              ENCODE ZSTD,

    -- ── Descriptive attributes ────────────────────────────────────────────────
    order_status            VARCHAR(30)     NOT NULL ENCODE ZSTD,
    order_priority          VARCHAR(20)              ENCODE ZSTD,
    ship_mode               VARCHAR(30)              ENCODE ZSTD,
    product_category        VARCHAR(100)             ENCODE ZSTD,
    product_name            VARCHAR(500)             ENCODE ZSTD,
    region                  VARCHAR(50)              ENCODE ZSTD,
    country                 VARCHAR(100)             ENCODE ZSTD,
    city                    VARCHAR(100)             ENCODE ZSTD,
    postal_code             VARCHAR(20)              ENCODE ZSTD,
    channel                 VARCHAR(30)              ENCODE ZSTD,

    -- ── Date columns ─────────────────────────────────────────────────────────
    order_date              DATE            NOT NULL ENCODE AZ64,
    ship_date               DATE                     ENCODE AZ64,
    order_year              SMALLINT        NOT NULL ENCODE AZ64,
    order_month             SMALLINT        NOT NULL ENCODE AZ64,
    order_quarter           SMALLINT        NOT NULL ENCODE AZ64,

    -- ── Additive measures ────────────────────────────────────────────────────
    quantity                INTEGER         NOT NULL ENCODE AZ64,
    unit_price              NUMERIC(12, 2)  NOT NULL ENCODE AZ64,
    discount                NUMERIC(5, 4)   NOT NULL DEFAULT 0 ENCODE AZ64,
    shipping_cost           NUMERIC(12, 2)           ENCODE AZ64,
    gross_revenue           NUMERIC(14, 2)  NOT NULL ENCODE AZ64,
    net_revenue             NUMERIC(14, 2)  NOT NULL ENCODE AZ64,
    discount_amount         NUMERIC(12, 2)           ENCODE AZ64,

    -- ── Semi-additive / derived measures ─────────────────────────────────────
    days_to_ship            SMALLINT                 ENCODE AZ64,
    is_late_shipment        BOOLEAN         NOT NULL DEFAULT FALSE ENCODE RAW,

    -- ── ETL audit ────────────────────────────────────────────────────────────
    _source_file            VARCHAR(1000)            ENCODE ZSTD,
    _glue_job_name          VARCHAR(200)             ENCODE ZSTD,
    _glue_run_id            CHAR(36)                 ENCODE ZSTD,
    _processed_at           TIMESTAMP       NOT NULL DEFAULT GETDATE() ENCODE AZ64
)
DISTSTYLE EVEN
COMPOUND SORTKEY (order_date, product_category, region, order_status);

COMMENT ON TABLE orders.fact_orders IS
    'Transaction fact table at order line item grain. '
    'DISTSTYLE EVEN distributes rows uniformly across all slices to prevent '
    'data skew (order_id cardinality is too high for KEY distribution). '
    'Compound sort on (order_date, product_category, region, order_status) '
    'optimises the most common BI filter patterns.';


-- ---------------------------------------------------------------------------
-- fact_orders_staging — Staging table for COPY + UPSERT pattern
-- Mirrors fact_orders exactly so DELETE + INSERT works without schema drift
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS orders.fact_orders_staging CASCADE;

CREATE TABLE orders.fact_orders_staging
    (LIKE orders.fact_orders)
DISTSTYLE EVEN;

COMMENT ON TABLE orders.fact_orders_staging IS
    'Temporary staging table for Redshift COPY + UPSERT. '
    'Data is COPY-ed here first, then merged into fact_orders via '
    'DELETE + INSERT (atomic transaction). Truncated after each ETL run.';


-- =============================================================================
-- 3. ETL Audit Log
-- =============================================================================

DROP TABLE IF EXISTS orders.etl_audit_log CASCADE;

CREATE TABLE orders.etl_audit_log
(
    audit_id                BIGINT IDENTITY(1,1)    ENCODE AZ64,
    run_id                  CHAR(36)        NOT NULL ENCODE ZSTD,
    job_name                VARCHAR(200)    NOT NULL ENCODE ZSTD,
    target_table            VARCHAR(200)    NOT NULL ENCODE ZSTD,
    partition_date          DATE                     ENCODE AZ64,
    rows_read               BIGINT                   ENCODE AZ64,
    rows_rejected           BIGINT                   ENCODE AZ64,
    rows_inserted           BIGINT                   ENCODE AZ64,
    rows_updated            BIGINT                   ENCODE AZ64,
    manifest_s3_uri         VARCHAR(1000)            ENCODE ZSTD,
    status                  VARCHAR(20)     NOT NULL ENCODE ZSTD,   -- RUNNING|SUCCESS|FAILED
    error_message           VARCHAR(4000)            ENCODE ZSTD,
    started_at              TIMESTAMP       NOT NULL DEFAULT GETDATE() ENCODE AZ64,
    completed_at            TIMESTAMP                ENCODE AZ64,
    duration_seconds        INTEGER                  ENCODE AZ64
)
DISTSTYLE ALL
SORTKEY (started_at);

COMMENT ON TABLE orders.etl_audit_log IS
    'ETL pipeline audit log. One row per Glue job run. '
    'DISTSTYLE ALL — small table, queried from all nodes.';


-- =============================================================================
-- 4. Views for BI / Downstream Consumers
-- =============================================================================

-- Latest-version customer join helper
CREATE OR REPLACE VIEW orders.v_current_customers AS
SELECT *
FROM orders.dim_customer
WHERE is_current = TRUE;

-- Revenue summary by month (used by Power BI DirectQuery)
CREATE OR REPLACE VIEW orders.v_monthly_revenue AS
SELECT
    fo.order_year,
    fo.order_month,
    fo.order_quarter,
    fo.product_category,
    fo.region,
    fo.channel,
    fo.order_status,
    COUNT(DISTINCT fo.order_id)     AS order_count,
    COUNT(DISTINCT fo.customer_id)  AS customer_count,
    SUM(fo.quantity)                AS total_units,
    SUM(fo.gross_revenue)           AS total_gross_revenue,
    SUM(fo.net_revenue)             AS total_net_revenue,
    SUM(fo.shipping_cost)           AS total_shipping_cost,
    AVG(fo.days_to_ship)            AS avg_days_to_ship,
    SUM(CASE WHEN fo.is_late_shipment THEN 1 ELSE 0 END) AS late_shipment_count,
    ROUND(
        SUM(CASE WHEN fo.is_late_shipment THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100,
    2)                              AS late_shipment_pct
FROM orders.fact_orders fo
GROUP BY 1,2,3,4,5,6,7;

COMMENT ON VIEW orders.v_monthly_revenue IS
    'Pre-aggregated monthly revenue view for BI performance. '
    'Avoids full fact table scans for common dashboard queries.';


-- =============================================================================
-- 5. Redshift COPY Command Template (run by Glue via Redshift Data API)
-- =============================================================================

/*
-- COPY from S3 manifest (executed by Glue job — do not run manually):
COPY orders.fact_orders_staging
FROM 's3://your-manifest-bucket/manifests/fact_orders/run_id=<run_id>/<ts>.json'
IAM_ROLE 'arn:aws:iam::123456789012:role/glue-redshift-etl-redshift-copy-role'
FORMAT AS PARQUET
MANIFEST
ACCEPTINVCHARS ' '
TRUNCATECOLUMNS
STATUPDATE ON
COMPUPDATE ON;
*/


-- =============================================================================
-- 6. UNLOAD Command Template (executed by Glue via Redshift Data API)
-- =============================================================================

/*
-- UNLOAD daily summary to S3 for Athena/QuickSight:
UNLOAD (
    'SELECT
        order_year, order_month, order_quarter,
        product_category, region, channel,
        COUNT(DISTINCT order_id)     AS order_count,
        SUM(net_revenue)             AS total_net_revenue,
        AVG(days_to_ship)            AS avg_days_to_ship
    FROM orders.fact_orders
    WHERE order_date >= CURRENT_DATE - 90
    GROUP BY 1,2,3,4,5,6'
)
TO 's3://your-unload-bucket/summaries/fact_orders/YYYY/MM/DD/'
IAM_ROLE 'arn:aws:iam::123456789012:role/glue-redshift-etl-redshift-copy-role'
FORMAT AS PARQUET
ALLOWOVERWRITE
PARALLEL ON
MAXFILESIZE 256 MB;
*/


-- =============================================================================
-- 7. Grants (apply after table creation)
-- =============================================================================

GRANT USAGE ON SCHEMA orders TO GROUP bi_users;
GRANT SELECT ON ALL TABLES IN SCHEMA orders TO GROUP bi_users;
GRANT SELECT ON orders.v_monthly_revenue TO GROUP bi_users;
GRANT SELECT ON orders.v_current_customers TO GROUP bi_users;

GRANT USAGE ON SCHEMA orders TO GROUP etl_service;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA orders TO GROUP etl_service;
