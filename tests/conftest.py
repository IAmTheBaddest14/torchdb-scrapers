import os
import pytest
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

TEST_SCHEMA = "scraper_staging_test"


@pytest.fixture(scope="session")
def supabase() -> Client:
    return create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
    )


@pytest.fixture(autouse=True)
def wipe_test_schema(supabase: Client):
    """Wipe all test tables before each test in dependency order."""
    for table in ["promotion_log", "extracted_products", "raw_pages", "crawl_runs"]:
        supabase.schema(TEST_SCHEMA).table(table).delete().neq("id", 0).execute()
    yield


@pytest.fixture
def repo(supabase: Client):
    from src.staging.repository import StagingRepository
    return StagingRepository(supabase, schema=TEST_SCHEMA)
