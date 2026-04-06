"""In-memory state stores. In production, replace with Redis or Delta Lake tables."""
import asyncio
import threading
from typing import Dict, Any

from app.models import MigrationSchedule, ScheduleJobExecution

# Source system metadata queries
SOURCE_METADATA_QUERIES = {
    "oracle": {
        "databases": "SELECT DISTINCT OWNER FROM ALL_TABLES WHERE OWNER NOT IN ('SYS','SYSTEM','OUTLN','DIP') ORDER BY OWNER",
        "schemas": "SELECT DISTINCT OWNER as schema_name FROM ALL_TABLES WHERE OWNER NOT IN ('SYS','SYSTEM') ORDER BY OWNER",
        "tables": "SELECT OWNER as schema_name, TABLE_NAME as table_name, 'TABLE' as object_type FROM ALL_TABLES WHERE OWNER = :schema",
        "views": "SELECT OWNER as schema_name, VIEW_NAME as view_name, TEXT as view_definition FROM ALL_VIEWS WHERE OWNER = :schema",
        "procedures": "SELECT OWNER as schema_name, OBJECT_NAME as proc_name, OBJECT_TYPE as proc_type FROM ALL_OBJECTS WHERE OBJECT_TYPE IN ('PROCEDURE','FUNCTION','PACKAGE') AND OWNER = :schema",
        "columns": "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE FROM ALL_TAB_COLUMNS WHERE OWNER = :schema AND TABLE_NAME = :table ORDER BY COLUMN_ID",
        "table_ddl": "SELECT DBMS_METADATA.GET_DDL('TABLE', :table, :schema) as ddl FROM DUAL"
    },
    "snowflake": {
        "databases": "SHOW DATABASES",
        "schemas": "SHOW SCHEMAS IN DATABASE {database}",
        "tables": "SHOW TABLES IN SCHEMA {database}.{schema}",
        "views": "SHOW VIEWS IN SCHEMA {database}.{schema}",
        "procedures": "SHOW PROCEDURES IN SCHEMA {database}.{schema}",
        "columns": "DESCRIBE TABLE {database}.{schema}.{table}",
        "table_ddl": "SELECT GET_DDL('TABLE', '{database}.{schema}.{table}') as ddl"
    },
    "sqlserver": {
        "databases": "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb') ORDER BY name",
        "schemas": "SELECT SCHEMA_NAME as schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('sys','INFORMATION_SCHEMA','guest') ORDER BY SCHEMA_NAME",
        "tables": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as table_name, 'TABLE' as object_type FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @schema",
        "views": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as view_name, VIEW_DEFINITION as view_definition FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = @schema",
        "procedures": "SELECT ROUTINE_SCHEMA as schema_name, ROUTINE_NAME as proc_name, ROUTINE_TYPE as proc_type, ROUTINE_DEFINITION as proc_definition FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = @schema",
        "columns": "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table ORDER BY ORDINAL_POSITION",
        "table_ddl": "EXEC sp_helptext @objname"
    },
    "teradata": {
        "databases": "SELECT DatabaseName FROM DBC.DatabasesV WHERE DBKind = 'D' ORDER BY DatabaseName",
        "schemas": "SELECT DatabaseName as schema_name FROM DBC.DatabasesV WHERE DBKind IN ('D','U') ORDER BY DatabaseName",
        "tables": "SELECT DatabaseName as schema_name, TableName as table_name, TableKind as object_type FROM DBC.TablesV WHERE DatabaseName = :schema AND TableKind IN ('T','O')",
        "views": "SELECT DatabaseName as schema_name, TableName as view_name, RequestText as view_definition FROM DBC.TablesV WHERE DatabaseName = :schema AND TableKind = 'V'",
        "procedures": "SELECT DatabaseName as schema_name, ProcedureName as proc_name, 'PROCEDURE' as proc_type FROM DBC.ProceduresV WHERE DatabaseName = :schema",
        "columns": "SELECT ColumnName, ColumnType, ColumnLength, DecimalTotalDigits, DecimalFractionalDigits, Nullable FROM DBC.ColumnsV WHERE DatabaseName = :schema AND TableName = :table ORDER BY ColumnId",
        "table_ddl": "SHOW TABLE {schema}.{table}"
    },
    "netezza": {
        "databases": "SELECT DATABASE FROM _V_DATABASE ORDER BY DATABASE",
        "schemas": "SELECT SCHEMA FROM _V_SCHEMA WHERE SCHEMA NOT LIKE 'SYSTEM%' ORDER BY SCHEMA",
        "tables": "SELECT SCHEMA as schema_name, TABLENAME as table_name, 'TABLE' as object_type FROM _V_TABLE WHERE SCHEMA = :schema",
        "views": "SELECT SCHEMA as schema_name, VIEWNAME as view_name, DEFINITION as view_definition FROM _V_VIEW WHERE SCHEMA = :schema",
        "procedures": "SELECT SCHEMA as schema_name, PROCEDURENAME as proc_name, 'PROCEDURE' as proc_type FROM _V_PROCEDURE WHERE SCHEMA = :schema",
        "columns": "SELECT ATTNAME as column_name, FORMAT_TYPE as data_type FROM _V_RELATION_COLUMN WHERE NAME = :table AND SCHEMA = :schema ORDER BY ATTNUM",
        "table_ddl": "\\d {schema}.{table}"
    },
    "synapse": {
        "databases": "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb') ORDER BY name",
        "schemas": "SELECT SCHEMA_NAME as schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('sys','INFORMATION_SCHEMA') ORDER BY SCHEMA_NAME",
        "tables": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as table_name, 'TABLE' as object_type FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @schema",
        "views": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as view_name, VIEW_DEFINITION as view_definition FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = @schema",
        "procedures": "SELECT ROUTINE_SCHEMA as schema_name, ROUTINE_NAME as proc_name, ROUTINE_TYPE as proc_type FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = @schema",
        "columns": "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table ORDER BY ORDINAL_POSITION",
        "table_ddl": "EXEC sp_helptext @objname"
    },
    "redshift": {
        "databases": "SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1','padb_harvest') ORDER BY datname",
        "schemas": "SELECT nspname as schema_name FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema' ORDER BY nspname",
        "tables": "SELECT schemaname as schema_name, tablename as table_name, 'TABLE' as object_type FROM pg_tables WHERE schemaname = :schema",
        "views": "SELECT schemaname as schema_name, viewname as view_name, definition as view_definition FROM pg_views WHERE schemaname = :schema",
        "procedures": "SELECT routine_schema as schema_name, routine_name as proc_name, routine_type as proc_type FROM information_schema.routines WHERE routine_schema = :schema",
        "columns": "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable FROM information_schema.columns WHERE table_schema = :schema AND table_name = :table ORDER BY ordinal_position",
        "table_ddl": "SELECT pg_get_ddl('table', '{schema}.{table}')"
    },
    "mysql": {
        "databases": "SHOW DATABASES",
        "schemas": "SELECT SCHEMA_NAME as schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('mysql','information_schema','performance_schema','sys') ORDER BY SCHEMA_NAME",
        "tables": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as table_name, 'TABLE' as object_type FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = :schema",
        "views": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as view_name, VIEW_DEFINITION as view_definition FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = :schema",
        "procedures": "SELECT ROUTINE_SCHEMA as schema_name, ROUTINE_NAME as proc_name, ROUTINE_TYPE as proc_type, ROUTINE_DEFINITION as proc_definition FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = :schema",
        "columns": "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table ORDER BY ORDINAL_POSITION",
        "table_ddl": "SHOW CREATE TABLE {schema}.{table}"
    }
}

# Migration job state tracking (in production, use Redis or Unity Catalog table)
migration_jobs: Dict[str, Dict[str, Any]] = {}
migration_locks: Dict[str, asyncio.Lock] = {}
active_connections: Dict[str, Dict[str, Any]] = {}
connection_lock = threading.Lock()

# Scheduling storage (in production, use Delta Lake table)
scheduled_jobs: Dict[str, MigrationSchedule] = {}
job_executions: Dict[str, ScheduleJobExecution] = {}

# Snapshot store (in production, use database)
snapshots: Dict[str, Dict[str, Any]] = {}

# Test job storage (in production, use Redis)
test_jobs: Dict[str, Dict[str, Any]] = {}
