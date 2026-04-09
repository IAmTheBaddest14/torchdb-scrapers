-- Add PDF manual columns to raw_pages in both schemas

DO $$
DECLARE
    schemas TEXT[] := ARRAY['scraper_staging', 'scraper_staging_test'];
    s TEXT;
BEGIN
    FOREACH s IN ARRAY schemas LOOP

        EXECUTE format('
            ALTER TABLE %I.raw_pages
                ADD COLUMN IF NOT EXISTS manual_pdf_url  TEXT,
                ADD COLUMN IF NOT EXISTS manual_pdf_text TEXT
        ', s);

    END LOOP;
END;
$$;
