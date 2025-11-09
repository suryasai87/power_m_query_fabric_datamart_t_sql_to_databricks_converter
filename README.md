# Power M Query / Fabric Datamart / T-SQL to Databricks Converter

A comprehensive tool for converting Power M Query, Microsoft Fabric Datamart queries, and T-SQL to Databricks SQL.

## Overview

This project provides automated conversion capabilities for migrating queries from:
- Power M Query (Power BI/Power Query)
- Microsoft Fabric Datamart
- T-SQL (SQL Server/Azure SQL)

To Databricks SQL with Unity Catalog support.

## Features

- Power M Query to Databricks SQL conversion
- Fabric Datamart to Databricks SQL migration
- T-SQL to Databricks SQL transformation
- Unity Catalog compatibility
- SQL Serverless Warehouse support
- Syntax validation and testing

## Project Structure

```
power_m_query_fabric_datamart_t_sql_to_databricks_converter/
│
├── converters/              # Conversion logic modules
│   ├── power_m/            # Power M Query converter
│   ├── fabric/             # Fabric Datamart converter
│   └── tsql/               # T-SQL converter
│
├── tests/                  # Test files
│   ├── sample_queries/     # Sample input queries
│   └── test_converters.py  # Unit tests
│
├── utils/                  # Utility functions
│   ├── databricks_client.py  # Databricks connection
│   └── sql_validator.py      # SQL validation
│
├── config.py               # Configuration settings
├── requirements.txt        # Python dependencies
├── .gitignore             # Git ignore rules
└── README.md              # This file
```

## Prerequisites

- Python 3.9+
- Databricks workspace access
- Databricks SQL Serverless Warehouse
- Unity Catalog enabled

## Configuration

The project uses the DEFAULT profile from your `~/.databrickscfg` file:

```ini
[DEFAULT]
host = https://fe-vm-hls-amer.cloud.databricks.com/
token = <your-token>
```

SQL Serverless Warehouse ID: `4b28691c780d9875`

## Installation

```bash
# Clone the repository
git clone https://github.com/suryasai87/power_m_query_fabric_datamart_t_sql_to_databricks_converter.git
cd power_m_query_fabric_datamart_t_sql_to_databricks_converter

# Install dependencies
pip install -r requirements.txt
```

## Usage

(Usage instructions will be added as development progresses)

## License

MIT License

## Author

Suryasai Turaga

## Status

🚧 **In Development** - Initial setup complete
