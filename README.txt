================================================================================
POWER M QUERY / FABRIC DATAMART / T-SQL TO DATABRICKS SQL CONVERTER
================================================================================

A comprehensive Python tool for converting SQL queries from Power M Query,
Microsoft Fabric Datamart, and T-SQL to Databricks SQL format.

Features:
- Automated conversion of T-SQL to Databricks SQL
- Power Query M language to Databricks SQL conversion
- Microsoft Fabric Datamart support
- Data type mapping (SQL Server -> Databricks)
- Date/time function conversion
- Unity Catalog support
- Databricks SQL Serverless testing
- Detailed migration reports

================================================================================
TABLE OF CONTENTS
================================================================================

1. Prerequisites
2. Installation & Setup
3. Configuration
4. Usage Examples
5. Running the Converter
6. Testing Converted Queries
7. Project Structure
8. Conversion Features
9. Troubleshooting
10. Deployment to Databricks Workspace

================================================================================
1. PREREQUISITES
================================================================================

Before using this tool, ensure you have:

- Python 3.8 or higher
- Access to Databricks workspace
- Databricks personal access token
- SQL Serverless Warehouse ID
- Git (for cloning the repository)

Databricks Prerequisites:
- Active Databricks workspace
- SQL Serverless Warehouse created
- Unity Catalog access (recommended)
- Profile configured in ~/.databrickscfg

================================================================================
2. INSTALLATION & SETUP
================================================================================

Step 1: Clone the Repository
-----------------------------
```
git clone https://github.com/suryasai87/power_m_query_fabric_datamart_t_sql_to_databricks_converter.git
cd power_m_query_fabric_datamart_t_sql_to_databricks_converter
```

Step 2: Create Virtual Environment
-----------------------------------
```
python3 -m venv venv

# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

Step 3: Install Dependencies
-----------------------------
```
pip install -r requirements.txt
```

This will install:
- databricks-sdk >= 0.18.0
- sqlparse >= 0.4.4
- sqlglot >= 20.0.0
- click >= 8.1.7
- rich >= 13.0.0
- pandas >= 2.0.0
- pytest >= 7.4.0 (for testing)
- And other required libraries

Step 4: Verify Installation
----------------------------
```
python convert_to_databricks.py --help
```

If successful, you should see the help menu with all available options.

================================================================================
3. CONFIGURATION
================================================================================

Step 1: Configure Databricks Profile
-------------------------------------
The tool uses the DEFAULT profile from ~/.databrickscfg by default.

Your ~/.databrickscfg should look like this:

```
[DEFAULT]
host = https://fe-vm-hls-amer.cloud.databricks.com/
token = your-databricks-token-here
```

Step 2: Update config.py (Optional)
------------------------------------
Edit the config.py file to customize:

```python
# Databricks Configuration
DATABRICKS_PROFILE = "DEFAULT"
WAREHOUSE_ID = "4b28691c780d9875"  # Your SQL Serverless Warehouse ID

# Unity Catalog Configuration
DEFAULT_CATALOG = "hls_amer_catalog"
DEFAULT_SCHEMA = "default"
```

To find your Warehouse ID:
1. Open Databricks workspace
2. Go to SQL -> SQL Warehouses
3. Click on your warehouse
4. Copy the ID from the URL or warehouse details

Step 3: Test Databricks Connection
-----------------------------------
```
python utils/databricks_client.py
```

Expected output:
```
═══════════════════════════════════════════════════════
Databricks Connection Test
═══════════════════════════════════════════════════════
INFO - Connected to Databricks using profile: DEFAULT
INFO - ✓ Connection test PASSED
INFO - Warehouse ID: 4b28691c780d9875
INFO - Profile: DEFAULT

✓ Connection successful!

Warehouse Information:
  Name: SQL Serverless Warehouse
  ID: 4b28691c780d9875
  State: RUNNING
  Type: PRO
═══════════════════════════════════════════════════════
```

================================================================================
4. USAGE EXAMPLES
================================================================================

Basic Conversion (No Testing)
------------------------------
```
python convert_to_databricks.py \
  --input-dir ./tests/sample_queries \
  --output-dir ./output
```

Conversion with Databricks Testing
-----------------------------------
```
python convert_to_databricks.py \
  --input-dir ./tests/sample_queries \
  --output-dir ./output \
  --test
```

Dry Run (Validate Syntax Only)
-------------------------------
```
python convert_to_databricks.py \
  --input-dir ./tests/sample_queries \
  --output-dir ./output \
  --test \
  --dry-run
```

With Custom Catalog and Schema
-------------------------------
```
python convert_to_databricks.py \
  --input-dir ./tests/sample_queries \
  --output-dir ./output \
  --catalog my_catalog \
  --schema my_schema \
  --test
```

Using Different Databricks Profile
-----------------------------------
```
python convert_to_databricks.py \
  --input-dir ./tests/sample_queries \
  --output-dir ./output \
  --connection-profile e2demofieldeng
```

================================================================================
5. RUNNING THE CONVERTER
================================================================================

Step 1: Prepare Input Files
----------------------------
Place your source SQL files in the input directory:

tests/sample_queries/
  ├── 0_work_orders_last_12_months.m      (Power M Query)
  ├── 2_simple_datamart_reporting.sql     (T-SQL with running sum)
  ├── 4_example_report_query.sql          (PowerBI report query)
  └── complex_tsql_example.sql            (Complex DDL & queries)

Supported file types:
- .sql  - T-SQL or Fabric Datamart SQL
- .m    - Power Query M language files

Step 2: Run the Converter
--------------------------
```
python convert_to_databricks.py \
  --input-dir ./tests/sample_queries \
  --output-dir ./databricks_output
```

Step 3: Review Output
---------------------
The tool generates:

databricks_output/
  ├── 0_work_orders_last_12_months_databricks.sql
  ├── 2_simple_datamart_reporting_databricks.sql
  ├── 4_example_report_query_databricks.sql
  ├── complex_tsql_example_databricks.sql
  └── migration_report.md

Step 4: Review Migration Report
--------------------------------
Open migration_report.md to see:
- Conversion summary (success/failure counts)
- Detailed conversion notes for each file
- Data type mappings applied
- Function conversions performed
- Test results (if --test was used)

================================================================================
6. TESTING CONVERTED QUERIES
================================================================================

Why Test?
---------
Testing ensures:
- Syntax is valid Databricks SQL
- Queries execute successfully
- No runtime errors

How to Test
-----------
Add the --test flag:

```
python convert_to_databricks.py \
  --input-dir ./tests/sample_queries \
  --output-dir ./output \
  --test
```

Test Process:
1. Connects to Databricks using configured profile
2. Tests connection with simple query
3. For each converted file:
   - Validates SQL syntax
   - Executes SELECT queries (safe)
   - Records results (passed/failed)
4. Generates test results in migration_report.md

Dry Run Testing:
----------------
To validate syntax without executing:

```
python convert_to_databricks.py \
  --input-dir ./tests/sample_queries \
  --output-dir ./output \
  --test \
  --dry-run
```

================================================================================
7. PROJECT STRUCTURE
================================================================================

power_m_query_fabric_datamart_t_sql_to_databricks_converter/
│
├── README.txt                          # This file
├── README.md                           # Original README
├── config.py                           # Configuration
├── requirements.txt                    # Dependencies
├── convert_to_databricks.py            # Main conversion script
│
├── converters/                         # Converter modules
│   ├── __init__.py
│   ├── tsql/
│   │   ├── __init__.py
│   │   └── converter.py                # T-SQL converter
│   ├── power_m/
│   │   ├── __init__.py
│   │   └── converter.py                # Power M Query converter
│   └── fabric/
│       ├── __init__.py
│       └── converter.py                # Fabric Datamart converter
│
├── utils/                              # Utility modules
│   ├── __init__.py
│   ├── databricks_client.py            # Databricks connection & testing
│   └── type_mappings.py                # Data type mappings
│
├── tests/
│   └── sample_queries/                 # Sample input files
│       ├── 0_work_orders_last_12_months.m
│       ├── 2_simple_datamart_reporting.sql
│       ├── 4_example_report_query.sql
│       └── complex_tsql_example.sql
│
└── output/                             # Generated output (created on run)
    ├── *_databricks.sql                # Converted SQL files
    └── migration_report.md             # Detailed report

================================================================================
8. CONVERSION FEATURES
================================================================================

T-SQL to Databricks SQL
------------------------
✓ Identifier conversion: [Table] -> `Table` or unquoted
✓ Date functions: GETDATE() -> CURRENT_TIMESTAMP()
✓ Date arithmetic: DATEADD(day, -7, GETDATE()) -> DATE_SUB(CURRENT_DATE(), 7)
✓ System functions: NEWID() -> UUID(), ISNULL -> COALESCE
✓ Data types: All SQL Server types mapped to Databricks
✓ Window functions: Preserved (natively supported)
✓ Constraints: PRIMARY KEY, NOT NULL, CHECK (informational)
✓ DEFAULT values: Converted appropriately
✓ Unity Catalog: Three-level namespace support

Data Type Mappings
------------------
SQL Server              -> Databricks
--------------------- ----  -------------------
INT, BIGINT, SMALLINT    ->  INT, BIGINT, SMALLINT
BIT                      ->  BOOLEAN
MONEY, SMALLMONEY        ->  DECIMAL(19,4), DECIMAL(10,4)
FLOAT, REAL              ->  DOUBLE, FLOAT
DATETIME, DATETIME2      ->  TIMESTAMP
DATE                     ->  DATE
VARCHAR, NVARCHAR, TEXT  ->  STRING
UNIQUEIDENTIFIER         ->  STRING
XML, JSON                ->  STRING
BINARY, VARBINARY        ->  BINARY

Power M Query to Databricks SQL
--------------------------------
✓ Source detection (Salesforce, SQL Server)
✓ Table name extraction
✓ Column selection (Table.SelectColumns -> SELECT)
✓ Date filtering: Date.IsInPreviousNDays(365) -> INTERVAL 12 MONTHS
✓ Sort operations: Table.Sort -> ORDER BY
✓ CREATE TABLE generation with appropriate structure

Fabric Datamart Support
-----------------------
✓ Uses T-SQL converter (Fabric uses T-SQL dialect)
✓ All T-SQL features supported
✓ Fabric-specific extensions can be added

================================================================================
9. TROUBLESHOOTING
================================================================================

Issue: "Module not found: databricks"
--------------------------------------
Solution:
```
pip install -r requirements.txt
```

Issue: "Connection test FAILED"
--------------------------------
Solutions:
1. Verify ~/.databrickscfg exists and has correct credentials
2. Check Databricks host URL (should end with .databricks.com or .databricksapps.com)
3. Verify token is valid (generate new token if expired)
4. Check warehouse ID is correct
5. Ensure SQL Serverless Warehouse is running

To find your token:
1. Open Databricks workspace
2. Click user icon (top right)
3. Settings -> Access Tokens
4. Generate new token

Issue: "Warehouse not found"
-----------------------------
Solutions:
1. Verify WAREHOUSE_ID in config.py
2. Check warehouse exists: databricks warehouses list
3. Ensure warehouse is started

Issue: Conversion errors
-------------------------
Solutions:
1. Check input file format (.sql or .m)
2. Review conversion log in migration_report.md
3. Complex queries may need manual review
4. Check for unsupported T-SQL features

Issue: Test failures
--------------------
Solutions:
1. Review specific error in migration_report.md
2. Check if tables/schemas exist in Databricks
3. Verify catalog and schema names
4. Some queries may need table creation first (DDL before DML)

================================================================================
10. DEPLOYMENT TO DATABRICKS WORKSPACE
================================================================================

Step 1: Upload to Databricks Workspace
---------------------------------------
Option A: Using Databricks CLI

```
# Upload entire project
databricks workspace import-dir \
  /Users/suryasai.turaga/repos/power_m_query_fabric_datamart_t_sql_to_databricks_converter \
  /Workspace/Users/suryasai.turaga@databricks.com/sql_converter
```

Option B: Using Databricks UI
1. Open Databricks workspace
2. Go to Workspace
3. Click "Import"
4. Upload convert_to_databricks.py and other files
5. Upload converters/ and utils/ directories

Step 2: Create Databricks Notebook
-----------------------------------
Create a Python notebook in Databricks:

```python
%pip install sqlparse sqlglot click rich

# Import the converter
import sys
sys.path.append('/Workspace/Users/suryasai.turaga@databricks.com/sql_converter')

from converters.tsql.converter import TSQLConverter

# Example conversion
tsql = """
SELECT
    CAST(CreatedDate AS DATE) AS WorkDate,
    SUM(Hours_Worked) AS TotalHours
FROM dbo.work_orders
WHERE CreatedDate >= DATEADD(day, -7, GETDATE())
GROUP BY CAST(CreatedDate AS DATE)
ORDER BY WorkDate DESC
"""

converter = TSQLConverter(catalog='hls_amer_catalog', schema='default')
databricks_sql, notes = converter.convert_query(tsql)

print(databricks_sql)
```

Step 3: Run Converted SQL
--------------------------
Create SQL notebook and execute converted queries:

```sql
-- Use the converted SQL from output files
CREATE OR REPLACE TABLE work_orders_last_12_months AS
SELECT *
FROM salesforce_source_table
WHERE CreatedDate >= CURRENT_DATE() - INTERVAL 12 MONTHS
ORDER BY CreatedDate DESC;
```

Step 4: Schedule Regular Conversions (Optional)
------------------------------------------------
Create a Databricks Job to run the converter periodically:

1. Go to Workflows -> Jobs
2. Create new job
3. Add Python task pointing to convert_to_databricks.py
4. Set schedule (daily, weekly, etc.)
5. Configure email notifications for failures

================================================================================
EXAMPLE WORKFLOW
================================================================================

Complete end-to-end example:

1. Configure Databricks:
   ```
   # Edit ~/.databrickscfg
   [DEFAULT]
   host = https://fe-vm-hls-amer.cloud.databricks.com/
   token = your-token-here
   ```

2. Test connection:
   ```
   python utils/databricks_client.py
   ```

3. Place source files in tests/sample_queries/

4. Run conversion with testing:
   ```
   python convert_to_databricks.py \
     --input-dir ./tests/sample_queries \
     --output-dir ./output \
     --catalog hls_amer_catalog \
     --schema default \
     --test
   ```

5. Review results:
   ```
   cat output/migration_report.md
   ```

6. Deploy converted SQL to Databricks:
   ```
   # Copy converted .sql files to Databricks
   databricks workspace import-dir \
     ./output \
     /Workspace/Users/suryasai.turaga@databricks.com/converted_sql
   ```

7. Execute in Databricks SQL Editor or notebook

================================================================================
SUPPORT & CONTRIBUTIONS
================================================================================

GitHub Repository:
https://github.com/suryasai87/power_m_query_fabric_datamart_t_sql_to_databricks_converter

For issues or questions:
1. Check Troubleshooting section above
2. Review migration_report.md for conversion details
3. Open an issue on GitHub

================================================================================
VERSION INFORMATION
================================================================================

Version: 1.0.0
Last Updated: 2025-11-09
Author: Claude Code with Databricks SDK
Python Version: 3.8+
Databricks SDK Version: >= 0.18.0

================================================================================
LICENSE
================================================================================

This project is open source. See repository for license details.

================================================================================
END OF DOCUMENTATION
================================================================================
