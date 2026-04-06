"""Shared dependency helpers for database and LLM connections."""
from databricks import sql
from app.config import DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
import logging

logger = logging.getLogger(__name__)

# Import openai with graceful fallback
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    openai = None

# Import JDBC and database connectors with graceful fallback
try:
    import jaydebeapi
    JAYDEBEAPI_AVAILABLE = True
except ImportError:
    JAYDEBEAPI_AVAILABLE = False
    jaydebeapi = None

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    psycopg2 = None

try:
    import pymysql
    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False
    pymysql = None

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake = None

# Import custom database connector and metadata extractor
try:
    from app.database_connector import DatabaseConnectionManager, get_jdbc_url, get_jdbc_driver_class, get_jdbc_driver_path, get_default_port
    from app.metadata_extractor import MetadataExtractor, SOURCE_METADATA_QUERIES as _SMQ
    CUSTOM_CONNECTORS_AVAILABLE = True
except ImportError as e:
    CUSTOM_CONNECTORS_AVAILABLE = False
    print(f"Warning: Could not import custom connectors: {e}")

# Import migration progress helpers (optional - for real-time progress tracking)
try:
    from migration_progress import (
        get_migration_lock, initialize_migration_job, update_migration_progress,
        add_migration_log, add_object_result, complete_migration_job, generate_sse_events
    )
    MIGRATION_PROGRESS_AVAILABLE = True
except ImportError:
    MIGRATION_PROGRESS_AVAILABLE = False


def get_databricks_connection():
    """Get a Databricks SQL connection."""
    return sql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


def get_llm_client():
    """Get OpenAI-compatible client for Databricks Foundation Models."""
    if not OPENAI_AVAILABLE or openai is None:
        raise RuntimeError("OpenAI library not available. Please ensure openai is installed.")
    return openai.OpenAI(
        api_key=DATABRICKS_TOKEN,
        base_url=f"{DATABRICKS_HOST}/serving-endpoints",
    )
