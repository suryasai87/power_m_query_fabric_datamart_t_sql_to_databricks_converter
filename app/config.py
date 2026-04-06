"""Configuration constants and environment variables."""
import os

# Configuration from environment variables
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://fe-vm-hls-amer.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/4b28691c780d9875")  # Serverless Starter Warehouse
LLM_AGENT_ENDPOINT = os.getenv("LLM_AGENT_ENDPOINT", "")

# Available Foundation Models
AVAILABLE_MODELS = {
    "llama-maverick": {
        "id": "databricks-llama-4-maverick",
        "name": "Llama 4 Maverick",
        "description": "Fast and efficient for general tasks (Default)",
        "pricing": {"input": 0.15, "output": 0.60}
    },
    "llama-70b": {
        "id": "databricks-meta-llama-3-3-70b-instruct",
        "name": "Llama 3.3 70B",
        "description": "Powerful model for complex reasoning",
        "pricing": {"input": 0.20, "output": 0.80}
    },
    "llama-405b": {
        "id": "databricks-meta-llama-3-1-405b-instruct",
        "name": "Llama 3.1 405B",
        "description": "Largest Llama model for most complex tasks",
        "pricing": {"input": 0.50, "output": 2.00}
    },
    "claude-sonnet-4-6": {
        "id": "databricks-claude-sonnet-4-6",
        "name": "Claude Sonnet 4.6",
        "description": "Latest Claude model with superior reasoning",
        "pricing": {"input": 3.00, "output": 15.00}
    },
    "claude-opus-4-6": {
        "id": "databricks-claude-opus-4-6",
        "name": "Claude Opus 4.6",
        "description": "Most powerful Claude model",
        "pricing": {"input": 15.00, "output": 75.00}
    },
    "gpt-5": {
        "id": "databricks-gpt-5",
        "name": "GPT-5",
        "description": "Latest OpenAI model",
        "pricing": {"input": 2.50, "output": 10.00}
    },
    "gemini-2-5-pro": {
        "id": "databricks-gemini-2-5-pro",
        "name": "Gemini 2.5 Pro",
        "description": "Google's most capable model",
        "pricing": {"input": 1.25, "output": 5.00}
    }
}

# Unity Catalog Volume for storing migration artifacts
MIGRATION_VOLUME = "hls_amer_catalog.dw_migration.dw_migration_volume"
SOURCE_DIRECTORY = "source"
ERROR_LOG_DIRECTORY = "dw_migration_error_log"
SNAPSHOT_DIRECTORY = "snapshots"

# Cost estimation constants
DBU_RATE_SERVERLESS = 0.70
STORAGE_RATE_GB_MONTH = 0.023
NETWORK_TRANSFER_RATE_GB = 0.02

WAREHOUSE_DBU_RATES = {
    "X-Small": 1,
    "Small": 2,
    "Medium": 4,
    "Large": 8,
    "X-Large": 16,
    "2X-Large": 32,
    "3X-Large": 64,
    "4X-Large": 128
}
