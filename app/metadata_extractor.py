"""
Metadata Extractor for DW Migration Assistant
Extracts schema metadata from various source databases
"""

import logging
from typing import Dict, Any, List, Optional
from database_connector import DatabaseConnectionManager

logger = logging.getLogger(__name__)

# Source system metadata queries
SOURCE_METADATA_QUERIES = {
    "oracle": {
        "databases": "SELECT DISTINCT OWNER FROM ALL_TABLES WHERE OWNER NOT IN ('SYS','SYSTEM','OUTLN','DIP') ORDER BY OWNER",
        "schemas": "SELECT DISTINCT OWNER as schema_name FROM ALL_TABLES WHERE OWNER NOT IN ('SYS','SYSTEM') ORDER BY OWNER",
        "tables": "SELECT OWNER as schema_name, TABLE_NAME as table_name, 'TABLE' as object_type FROM ALL_TABLES WHERE OWNER = :schema",
        "views": "SELECT OWNER as schema_name, VIEW_NAME as view_name, TEXT as view_definition FROM ALL_VIEWS WHERE OWNER = :schema",
        "procedures": "SELECT OWNER as schema_name, OBJECT_NAME as proc_name, OBJECT_TYPE as proc_type FROM ALL_OBJECTS WHERE OBJECT_TYPE IN ('PROCEDURE','FUNCTION','PACKAGE') AND OWNER = :schema",
        "columns": "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE FROM ALL_TAB_COLUMNS WHERE OWNER = :schema AND TABLE_NAME = :table ORDER BY COLUMN_ID",
        "table_ddl": "SELECT DBMS_METADATA.GET_DDL('TABLE', :table, :schema) as ddl FROM DUAL",
        "table_count": "SELECT COUNT(*) as row_count FROM {schema}.{table}"
    },
    "snowflake": {
        "databases": "SHOW DATABASES",
        "schemas": "SHOW SCHEMAS IN DATABASE {database}",
        "tables": "SHOW TABLES IN SCHEMA {database}.{schema}",
        "views": "SHOW VIEWS IN SCHEMA {database}.{schema}",
        "procedures": "SHOW PROCEDURES IN SCHEMA {database}.{schema}",
        "columns": "DESCRIBE TABLE {database}.{schema}.{table}",
        "table_ddl": "SELECT GET_DDL('TABLE', '{database}.{schema}.{table}') as ddl",
        "table_count": "SELECT COUNT(*) as row_count FROM {database}.{schema}.{table}"
    },
    "sqlserver": {
        "databases": "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb') ORDER BY name",
        "schemas": "SELECT SCHEMA_NAME as schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('sys','INFORMATION_SCHEMA','guest') ORDER BY SCHEMA_NAME",
        "tables": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as table_name, 'TABLE' as object_type FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @schema",
        "views": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as view_name, VIEW_DEFINITION as view_definition FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = @schema",
        "procedures": "SELECT ROUTINE_SCHEMA as schema_name, ROUTINE_NAME as proc_name, ROUTINE_TYPE as proc_type, ROUTINE_DEFINITION as proc_definition FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = @schema",
        "columns": "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table ORDER BY ORDINAL_POSITION",
        "table_ddl": "EXEC sp_helptext @objname",
        "table_count": "SELECT COUNT(*) as row_count FROM [{schema}].[{table}]"
    },
    "teradata": {
        "databases": "SELECT DatabaseName FROM DBC.DatabasesV WHERE DBKind = 'D' ORDER BY DatabaseName",
        "schemas": "SELECT DatabaseName as schema_name FROM DBC.DatabasesV WHERE DBKind IN ('D','U') ORDER BY DatabaseName",
        "tables": "SELECT DatabaseName as schema_name, TableName as table_name, TableKind as object_type FROM DBC.TablesV WHERE DatabaseName = :schema AND TableKind IN ('T','O')",
        "views": "SELECT DatabaseName as schema_name, TableName as view_name, RequestText as view_definition FROM DBC.TablesV WHERE DatabaseName = :schema AND TableKind = 'V'",
        "procedures": "SELECT DatabaseName as schema_name, ProcedureName as proc_name, 'PROCEDURE' as proc_type FROM DBC.ProceduresV WHERE DatabaseName = :schema",
        "columns": "SELECT ColumnName, ColumnType, ColumnLength, DecimalTotalDigits, DecimalFractionalDigits, Nullable FROM DBC.ColumnsV WHERE DatabaseName = :schema AND TableName = :table ORDER BY ColumnId",
        "table_ddl": "SHOW TABLE {schema}.{table}",
        "table_count": "SELECT COUNT(*) as row_count FROM {schema}.{table}"
    },
    "netezza": {
        "databases": "SELECT DATABASE FROM _V_DATABASE ORDER BY DATABASE",
        "schemas": "SELECT SCHEMA FROM _V_SCHEMA WHERE SCHEMA NOT LIKE 'SYSTEM%' ORDER BY SCHEMA",
        "tables": "SELECT SCHEMA as schema_name, TABLENAME as table_name, 'TABLE' as object_type FROM _V_TABLE WHERE SCHEMA = :schema",
        "views": "SELECT SCHEMA as schema_name, VIEWNAME as view_name, DEFINITION as view_definition FROM _V_VIEW WHERE SCHEMA = :schema",
        "procedures": "SELECT SCHEMA as schema_name, PROCEDURENAME as proc_name, 'PROCEDURE' as proc_type FROM _V_PROCEDURE WHERE SCHEMA = :schema",
        "columns": "SELECT ATTNAME as column_name, FORMAT_TYPE as data_type FROM _V_RELATION_COLUMN WHERE NAME = :table AND SCHEMA = :schema ORDER BY ATTNUM",
        "table_ddl": "\\d {schema}.{table}",
        "table_count": "SELECT COUNT(*) as row_count FROM {schema}.{table}"
    },
    "synapse": {
        "databases": "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb') ORDER BY name",
        "schemas": "SELECT SCHEMA_NAME as schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('sys','INFORMATION_SCHEMA') ORDER BY SCHEMA_NAME",
        "tables": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as table_name, 'TABLE' as object_type FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @schema",
        "views": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as view_name, VIEW_DEFINITION as view_definition FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = @schema",
        "procedures": "SELECT ROUTINE_SCHEMA as schema_name, ROUTINE_NAME as proc_name, ROUTINE_TYPE as proc_type FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = @schema",
        "columns": "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table ORDER BY ORDINAL_POSITION",
        "table_ddl": "EXEC sp_helptext @objname",
        "table_count": "SELECT COUNT(*) as row_count FROM [{schema}].[{table}]"
    },
    "redshift": {
        "databases": "SELECT datname FROM pg_database WHERE datname NOT IN ('template0','template1','padb_harvest') ORDER BY datname",
        "schemas": "SELECT nspname as schema_name FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema' ORDER BY nspname",
        "tables": "SELECT schemaname as schema_name, tablename as table_name, 'TABLE' as object_type FROM pg_tables WHERE schemaname = :schema",
        "views": "SELECT schemaname as schema_name, viewname as view_name, definition as view_definition FROM pg_views WHERE schemaname = :schema",
        "procedures": "SELECT routine_schema as schema_name, routine_name as proc_name, routine_type as proc_type FROM information_schema.routines WHERE routine_schema = :schema",
        "columns": "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable FROM information_schema.columns WHERE table_schema = :schema AND table_name = :table ORDER BY ordinal_position",
        "table_ddl": "SELECT pg_get_ddl('table', '{schema}.{table}')",
        "table_count": "SELECT COUNT(*) as row_count FROM {schema}.{table}"
    },
    "mysql": {
        "databases": "SHOW DATABASES",
        "schemas": "SELECT SCHEMA_NAME as schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('mysql','information_schema','performance_schema','sys') ORDER BY SCHEMA_NAME",
        "tables": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as table_name, 'TABLE' as object_type FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = :schema",
        "views": "SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as view_name, VIEW_DEFINITION as view_definition FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = :schema",
        "procedures": "SELECT ROUTINE_SCHEMA as schema_name, ROUTINE_NAME as proc_name, ROUTINE_TYPE as proc_type, ROUTINE_DEFINITION as proc_definition FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = :schema",
        "columns": "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table ORDER BY ORDINAL_POSITION",
        "table_ddl": "SHOW CREATE TABLE {schema}.{table}",
        "table_count": "SELECT COUNT(*) as row_count FROM `{schema}`.`{table}`"
    }
}


class MetadataExtractor:
    """Extract metadata from source databases"""

    @staticmethod
    def extract_schemas(conn_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract list of schemas/databases from source system"""
        source_type = conn_info["source_type"]
        queries = SOURCE_METADATA_QUERIES.get(source_type, {})

        if "schemas" not in queries:
            logger.warning(f"Schema query not defined for {source_type}")
            return []

        try:
            query = queries["schemas"]
            # For Snowflake, format the database name
            if source_type == "snowflake":
                query = query.format(database=conn_info["database"])

            results = DatabaseConnectionManager.execute_query(conn_info, query)
            logger.info(f"Extracted {len(results)} schemas from {source_type}")
            return results
        except Exception as e:
            logger.error(f"Failed to extract schemas: {str(e)}")
            raise

    @staticmethod
    def extract_tables(conn_info: Dict[str, Any], schema: str) -> List[Dict[str, Any]]:
        """Extract list of tables from a schema"""
        source_type = conn_info["source_type"]
        queries = SOURCE_METADATA_QUERIES.get(source_type, {})

        if "tables" not in queries:
            logger.warning(f"Tables query not defined for {source_type}")
            return []

        try:
            query = queries["tables"]

            # Handle different parameter styles
            if source_type in ["snowflake"]:
                query = query.format(database=conn_info["database"], schema=schema)
                results = DatabaseConnectionManager.execute_query(conn_info, query)
            elif source_type in ["oracle", "teradata", "netezza", "mysql", "redshift"]:
                results = DatabaseConnectionManager.execute_query(conn_info, query, {"schema": schema})
            else:  # SQL Server, Synapse
                results = DatabaseConnectionManager.execute_query(conn_info, query, {"schema": schema})

            logger.info(f"Extracted {len(results)} tables from schema {schema}")
            return results
        except Exception as e:
            logger.error(f"Failed to extract tables from schema {schema}: {str(e)}")
            raise

    @staticmethod
    def extract_views(conn_info: Dict[str, Any], schema: str) -> List[Dict[str, Any]]:
        """Extract list of views from a schema"""
        source_type = conn_info["source_type"]
        queries = SOURCE_METADATA_QUERIES.get(source_type, {})

        if "views" not in queries:
            logger.warning(f"Views query not defined for {source_type}")
            return []

        try:
            query = queries["views"]

            # Handle different parameter styles
            if source_type in ["snowflake"]:
                query = query.format(database=conn_info["database"], schema=schema)
                results = DatabaseConnectionManager.execute_query(conn_info, query)
            elif source_type in ["oracle", "teradata", "netezza", "mysql", "redshift"]:
                results = DatabaseConnectionManager.execute_query(conn_info, query, {"schema": schema})
            else:  # SQL Server, Synapse
                results = DatabaseConnectionManager.execute_query(conn_info, query, {"schema": schema})

            logger.info(f"Extracted {len(results)} views from schema {schema}")
            return results
        except Exception as e:
            logger.error(f"Failed to extract views from schema {schema}: {str(e)}")
            return []  # Views are optional, don't fail the whole extraction

    @staticmethod
    def extract_procedures(conn_info: Dict[str, Any], schema: str) -> List[Dict[str, Any]]:
        """Extract list of stored procedures from a schema"""
        source_type = conn_info["source_type"]
        queries = SOURCE_METADATA_QUERIES.get(source_type, {})

        if "procedures" not in queries:
            logger.warning(f"Procedures query not defined for {source_type}")
            return []

        try:
            query = queries["procedures"]

            # Handle different parameter styles
            if source_type in ["snowflake"]:
                query = query.format(database=conn_info["database"], schema=schema)
                results = DatabaseConnectionManager.execute_query(conn_info, query)
            elif source_type in ["oracle", "teradata", "netezza", "mysql", "redshift"]:
                results = DatabaseConnectionManager.execute_query(conn_info, query, {"schema": schema})
            else:  # SQL Server, Synapse
                results = DatabaseConnectionManager.execute_query(conn_info, query, {"schema": schema})

            logger.info(f"Extracted {len(results)} procedures from schema {schema}")
            return results
        except Exception as e:
            logger.error(f"Failed to extract procedures from schema {schema}: {str(e)}")
            return []  # Procedures are optional, don't fail the whole extraction

    @staticmethod
    def extract_columns(conn_info: Dict[str, Any], schema: str, table: str) -> List[Dict[str, Any]]:
        """Extract column information for a table"""
        source_type = conn_info["source_type"]
        queries = SOURCE_METADATA_QUERIES.get(source_type, {})

        if "columns" not in queries:
            logger.warning(f"Columns query not defined for {source_type}")
            return []

        try:
            query = queries["columns"]

            # Handle different parameter styles
            if source_type in ["snowflake"]:
                query = query.format(database=conn_info["database"], schema=schema, table=table)
                results = DatabaseConnectionManager.execute_query(conn_info, query)
            elif source_type in ["oracle", "teradata", "netezza", "mysql", "redshift"]:
                results = DatabaseConnectionManager.execute_query(conn_info, query, {"schema": schema, "table": table})
            else:  # SQL Server, Synapse
                results = DatabaseConnectionManager.execute_query(conn_info, query, {"schema": schema, "table": table})

            logger.info(f"Extracted {len(results)} columns from table {schema}.{table}")
            return results
        except Exception as e:
            logger.error(f"Failed to extract columns from table {schema}.{table}: {str(e)}")
            return []

    @staticmethod
    def extract_table_ddl(conn_info: Dict[str, Any], schema: str, table: str) -> Optional[str]:
        """Extract DDL for a table"""
        source_type = conn_info["source_type"]
        queries = SOURCE_METADATA_QUERIES.get(source_type, {})

        if "table_ddl" not in queries:
            logger.warning(f"Table DDL query not defined for {source_type}")
            return None

        try:
            query = queries["table_ddl"]

            # Handle different parameter styles
            if source_type in ["snowflake", "netezza", "teradata"]:
                query = query.format(database=conn_info.get("database", ""), schema=schema, table=table)
                results = DatabaseConnectionManager.execute_query(conn_info, query)
            elif source_type in ["oracle"]:
                results = DatabaseConnectionManager.execute_query(conn_info, query, {"schema": schema, "table": table})
            elif source_type in ["sqlserver", "synapse"]:
                # For SQL Server, sp_helptext requires the full object name
                objname = f"{schema}.{table}"
                results = DatabaseConnectionManager.execute_query(conn_info, query, {"objname": objname})
            elif source_type in ["mysql"]:
                query = query.format(schema=schema, table=table)
                results = DatabaseConnectionManager.execute_query(conn_info, query)
            else:
                query = query.format(schema=schema, table=table)
                results = DatabaseConnectionManager.execute_query(conn_info, query)

            if results and len(results) > 0:
                # Get the first column value from the first row
                first_key = list(results[0].keys())[0]
                ddl = results[0][first_key]
                logger.info(f"Extracted DDL for table {schema}.{table}")
                return ddl
            return None
        except Exception as e:
            logger.error(f"Failed to extract DDL for table {schema}.{table}: {str(e)}")
            return None

    @staticmethod
    def get_table_row_count(conn_info: Dict[str, Any], schema: str, table: str) -> int:
        """Get row count for a table"""
        source_type = conn_info["source_type"]
        queries = SOURCE_METADATA_QUERIES.get(source_type, {})

        if "table_count" not in queries:
            logger.warning(f"Table count query not defined for {source_type}")
            return 0

        try:
            query = queries["table_count"].format(schema=schema, table=table, database=conn_info.get("database", ""))
            results = DatabaseConnectionManager.execute_query(conn_info, query)

            if results and len(results) > 0:
                # Get the count from first row
                count_key = "row_count" if "row_count" in results[0] else list(results[0].keys())[0]
                count = results[0][count_key]
                logger.info(f"Table {schema}.{table} has {count} rows")
                return int(count)
            return 0
        except Exception as e:
            logger.error(f"Failed to get row count for table {schema}.{table}: {str(e)}")
            return 0
