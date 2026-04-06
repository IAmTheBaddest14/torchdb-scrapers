-- Create scraper_staging schema
CREATE SCHEMA IF NOT EXISTS scraper_staging;

-- Create scraper_staging_test schema (used by tests, wiped between runs)
CREATE SCHEMA IF NOT EXISTS scraper_staging_test;

-- Helper to create tables in a given schema
-- We create the same tables in both schemas

DO $$
DECLARE
    schemas TEXT[] := ARRAY['scraper_staging', 'scraper_staging_test'];
    s TEXT;
BEGIN
    FOREACH s IN ARRAY schemas LOOP

        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.crawl_runs (
                id              BIGSERIAL PRIMARY KEY,
                brand           TEXT NOT NULL,
                started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at    TIMESTAMPTZ,
                pages_crawled   INTEGER,
                scraper_version TEXT NOT NULL
            )
        ', s);

        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.raw_pages (
                id                  BIGSERIAL PRIMARY KEY,
                crawl_run_id        BIGINT NOT NULL REFERENCES %I.crawl_runs(id),
                url                 TEXT NOT NULL,
                markdown            TEXT,
                image_urls          JSONB NOT NULL DEFAULT ''[]''::jsonb,
                raw_variant_data    JSONB,
                crawled_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                scraper_version     TEXT NOT NULL
            )
        ', s, s);

        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.extracted_products (
                id                          BIGSERIAL PRIMARY KEY,
                raw_page_id                 BIGINT NOT NULL REFERENCES %I.raw_pages(id),
                brand                       TEXT NOT NULL,
                model                       TEXT NOT NULL,
                configuration_graph         JSONB NOT NULL,
                confidence_score            NUMERIC(5,2) NOT NULL,
                confidence_tier             TEXT NOT NULL CHECK (confidence_tier IN (''high'', ''medium'', ''low'')),
                extraction_prompt_version   TEXT NOT NULL,
                extracted_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        ', s, s);

        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.promotion_log (
                id                      BIGSERIAL PRIMARY KEY,
                extracted_product_id    BIGINT NOT NULL REFERENCES %I.extracted_products(id),
                torchdb_entity_type     TEXT,
                torchdb_entity_id       BIGINT,
                action                  TEXT NOT NULL CHECK (action IN (''insert'', ''update'', ''skip'', ''rejected'', ''review-required'')),
                promoted_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                promoted_by             TEXT,
                diff_summary            TEXT
            )
        ', s, s);

    END LOOP;
END;
$$;
