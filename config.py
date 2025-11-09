"""
Configuration file for Databricks connection and converter settings.

This file uses the DEFAULT profile from ~/.databrickscfg
"""

import os
from pathlib import Path

# Databricks Configuration
# Uses DEFAULT profile from ~/.databrickscfg
DATABRICKS_HOST = "https://fe-vm-hls-amer.cloud.databricks.com/"
DATABRICKS_PROFILE = "DEFAULT"

# SQL Serverless Warehouse
WAREHOUSE_ID = "4b28691c780d9875"

# Unity Catalog Configuration (to be updated as needed)
DEFAULT_CATALOG = "hls_amer_catalog"
DEFAULT_SCHEMA = "default"

# Project Paths
PROJECT_ROOT = Path(__file__).parent
CONVERTERS_DIR = PROJECT_ROOT / "converters"
TESTS_DIR = PROJECT_ROOT / "tests"
SAMPLE_QUERIES_DIR = TESTS_DIR / "sample_queries"
OUTPUT_DIR = PROJECT_ROOT / "output"

# Converter Settings
SUPPORTED_SOURCES = ["power_m", "fabric", "tsql"]

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Create directories if they don't exist
OUTPUT_DIR.mkdir(exist_ok=True)

# Environment Variables (optional overrides)
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")  # Optional: can override token
