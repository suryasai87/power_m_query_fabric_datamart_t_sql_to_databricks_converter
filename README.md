# DW Migration Assistant

A full-stack Databricks App for migrating data warehouse workloads to Databricks SQL. Converts T-SQL, Power M Query, and Fabric Datamart schemas using **sqlglot AST transpilation** and **Databricks Foundation Model APIs** (LLM-powered).

**Live App:** [dw-migration-assistant](https://dw-migration-assistant-1602460480284688.aws.databricksapps.com)

## Features

| Category | Capabilities |
|----------|-------------|
| **SQL Translation** | T-SQL/Synapse to Databricks SQL via sqlglot AST + LLM fallback. Power M Query and Fabric Datamart conversion. |
| **Source Connectivity** | Direct connections to SQL Server, PostgreSQL, MySQL, Snowflake, Oracle, Redshift via JDBC and native drivers. |
| **Unity Catalog** | Browse catalogs, schemas, tables, columns. Generate DDL with `USING DELTA`. |
| **Bulk Migration** | Multi-table batch conversion with progress streaming (SSE). |
| **Schema Comparison** | Source vs target diff with column-level change detection. |
| **Cost Estimation** | Compute, storage, and migration cost projections with platform comparison. |
| **Job Scheduling** | Cron-based scheduled migrations (daily/weekly/monthly/custom). |
| **Query Testing** | Execute translated queries, batch test, compare source vs target results. |
| **Rollback** | Delta Lake time-travel snapshots, diff, validate, restore. |
| **Foundation Models** | 7 LLM models via Databricks Model Serving (Claude, Llama, GPT, Gemini). |

## Architecture

```
dw-migration-assistant/
├── app/                        # FastAPI backend (modular)
│   ├── main.py                 # App wiring, CORS, static file serving
│   ├── config.py               # Environment config, model registry
│   ├── models.py               # Pydantic request/response models
│   ├── utils.py                # Helpers (cost calc, SQL validation)
│   ├── state.py                # In-memory stores (jobs, connections)
│   ├── dependencies.py         # Shared DB/LLM connection factories
│   ├── database_connector.py   # JDBC/native source DB connections
│   ├── metadata_extractor.py   # Source schema metadata extraction
│   └── routers/                # API route modules (10 routers)
│       ├── health.py           # /api/health, /api/debug, /api/models
│       ├── translation.py      # /api/translate-sql, /api/convert-ddl
│       ├── catalog.py          # /api/catalogs, schemas, tables
│       ├── connection.py       # /api/connect/test, extract-inventory
│       ├── migration.py        # /api/migrate/bulk, start, progress
│       ├── schedule.py         # /api/schedule CRUD, run-now
│       ├── compare.py          # /api/compare/schemas, tables
│       ├── cost.py             # /api/estimate/migration, storage
│       ├── testing.py          # /api/test/query, batch, compare
│       └── rollback.py         # /api/rollback/snapshot, restore
├── converters/                 # SQL conversion engines
│   ├── tsql/converter.py       # sqlglot-based T-SQL transpiler
│   ├── power_m/converter.py    # Power M Query parser
│   └── fabric/converter.py     # Fabric Datamart converter
├── frontend/                   # React 19 + Vite + MUI v6 + Recharts
│   ├── src/pages/              # 11 tab pages
│   └── vite.config.ts          # Builds to ../static/
├── utils/                      # Legacy CLI utilities
│   ├── type_mappings.py        # SQL Server -> Databricks type map
│   └── databricks_client.py    # Databricks SDK client
├── tests/                      # Test suites
│   ├── test_converters.py      # Converter unit tests (pytest)
│   └── sample_queries/         # Sample T-SQL, M query files
├── static/                     # Built React SPA (gitignored)
├── app.yaml                    # Databricks Apps deployment config
├── requirements.txt            # Python dependencies
└── convert_to_databricks.py    # Legacy CLI converter
```

## Quick Start

### Local Development

```bash
# Clone
git clone https://github.com/suryasai87/power_m_query_fabric_datamart_t_sql_to_databricks_converter.git
cd power_m_query_fabric_datamart_t_sql_to_databricks_converter

# Backend
pip install -r requirements.txt
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
uvicorn app.main:app --host 0.0.0.0 --port 8000

# Frontend (separate terminal)
cd frontend
npm install
npm run dev    # Dev server with API proxy to :8000
npm run build  # Build to ../static/
npm test       # Run Vitest
```

### Deploy to Databricks Apps

```bash
# Build frontend
cd frontend && npm run build && cd ..

# Deploy via Databricks CLI
databricks apps deploy dw-migration-assistant --profile DEFAULT
```

## API Endpoints (40+)

| Group | Endpoints | Description |
|-------|----------|-------------|
| Health | 4 | Health check, debug info, models list, warehouse status |
| Translation | 6 | SQL translate, execute, DDL convert, AI suggestions |
| Catalog | 5 | Unity Catalog browser (catalogs, schemas, tables, columns) |
| Connection | 5 | Source DB connect, test, extract inventory, disconnect |
| Migration | 8 | Bulk migrate, start/cancel, progress stream, job management |
| Schedule | 8 | Create/update/delete schedules, run-now, execution history |
| Compare | 3 | Schema comparison, table diff, data type mapping |
| Cost | 4 | Migration/storage/compute estimation, platform comparison |
| Testing | 4 | Query test, batch test, results comparison |
| Rollback | 6 | Snapshot create/list/diff/validate/restore/delete |

## Supported Source Platforms

- SQL Server / Azure SQL / Synapse Analytics
- Microsoft Fabric Datamart
- Power BI (Power M Query)
- PostgreSQL
- MySQL
- Snowflake
- Oracle Data Warehouse
- Amazon Redshift

## Foundation Models

| Model | ID | Use Case |
|-------|----|----------|
| Llama 4 Maverick | databricks-llama-4-maverick | Fast general tasks (default) |
| Llama 3.3 70B | databricks-meta-llama-3-3-70b-instruct | Complex reasoning |
| Llama 3.1 405B | databricks-meta-llama-3-1-405b-instruct | Most complex tasks |
| Claude Sonnet 4.6 | databricks-claude-sonnet-4-6 | Superior reasoning |
| Claude Opus 4.6 | databricks-claude-opus-4-6 | Most powerful |
| GPT-5 | databricks-gpt-5 | OpenAI latest |
| Gemini 2.5 Pro | databricks-gemini-2-5-pro | Google's most capable |

## Testing

```bash
# Backend tests
pytest tests/ -v

# Frontend tests
cd frontend && npm test

# Converter tests
pytest tests/test_converters.py -v
```

## Authors

- **Anand Rao** (anand.rao@databricks.com) - Lead Developer
- **Surya Sai Turaga** (suryasai.turaga@databricks.com) - Co-Developer

## Links

- [Confluence Documentation](https://databricks.atlassian.net/wiki/spaces/FE/pages/6177849804)
- [FEIP-5698](https://databricks.atlassian.net/browse/FEIP-5698)
- [Databricks App](https://dw-migration-assistant-1602460480284688.aws.databricksapps.com)

## License

Internal - Databricks Field Engineering
