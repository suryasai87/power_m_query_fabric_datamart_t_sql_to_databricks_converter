"""
T-SQL to Databricks SQL Converter.

This module converts T-SQL queries and DDL statements to Databricks SQL.
"""

import re
import logging
from typing import Dict, List, Tuple
import sqlparse
from utils.type_mappings import (
    map_sql_server_type,
    TSQL_FUNCTION_MAPPINGS,
    DATE_FUNCTION_PATTERNS
)

logger = logging.getLogger(__name__)


class TSQLConverter:
    """
    Converter for T-SQL to Databricks SQL.
    """

    def __init__(self, catalog: str = None, schema: str = None):
        """
        Initialize T-SQL converter.

        Args:
            catalog: Target Databricks Unity Catalog name
            schema: Target schema name
        """
        self.catalog = catalog
        self.schema = schema
        self.conversion_log = []

    def convert_query(self, tsql_query: str) -> Tuple[str, List[Dict]]:
        """
        Convert T-SQL query to Databricks SQL.

        Args:
            tsql_query: T-SQL query string

        Returns:
            Tuple of (converted_query, conversion_notes)
        """
        self.conversion_log = []
        converted = tsql_query

        # Step 1: Convert identifiers (brackets to backticks)
        converted = self._convert_identifiers(converted)

        # Step 2: Convert date/time functions
        converted = self._convert_date_functions(converted)

        # Step 3: Convert system functions
        converted = self._convert_system_functions(converted)

        # Step 4: Convert table references
        converted = self._convert_table_references(converted)

        # Step 5: Clean up formatting
        converted = self._clean_format(converted)

        return converted, self.conversion_log

    def convert_ddl(self, tsql_ddl: str) -> Tuple[str, List[Dict]]:
        """
        Convert T-SQL DDL (CREATE TABLE) to Databricks SQL.

        Args:
            tsql_ddl: T-SQL DDL statement

        Returns:
            Tuple of (converted_ddl, conversion_notes)
        """
        self.conversion_log = []
        converted = tsql_ddl

        # Step 1: Convert identifiers
        converted = self._convert_identifiers(converted)

        # Step 2: Convert data types
        converted = self._convert_data_types(converted)

        # Step 3: Convert constraints
        converted = self._convert_constraints(converted)

        # Step 4: Convert DEFAULT values
        converted = self._convert_default_values(converted)

        # Step 5: Add USING DELTA clause
        if 'CREATE TABLE' in converted.upper() and 'USING DELTA' not in converted.upper():
            # Find the end of the CREATE TABLE statement
            if ')' in converted:
                last_paren = converted.rfind(')')
                converted = converted[:last_paren+1] + ' USING DELTA' + converted[last_paren+1:]
                self._log_conversion('Added USING DELTA clause for Delta Lake table format')

        return converted, self.conversion_log

    def _convert_identifiers(self, sql: str) -> str:
        """
        Convert SQL Server bracket identifiers [table] to Databricks backticks `table`.
        Also handles [schema].[table] patterns.

        Args:
            sql: SQL string with bracket identifiers

        Returns:
            SQL with backtick identifiers
        """
        # Pattern: [identifier] or [schema].[table]
        def replace_bracket(match):
            identifier = match.group(1)
            # Check if identifier has spaces or special chars
            if ' ' in identifier or any(c in identifier for c in ['-', '/', '\\']):
                self._log_conversion(f'Converted bracketed identifier [{identifier}] to backticks')
                return f'`{identifier}`'
            else:
                # No special chars, can use without backticks
                self._log_conversion(f'Removed unnecessary brackets from [{identifier}]')
                return identifier

        # Replace [identifier] patterns
        converted = re.sub(r'\[([^\]]+)\]', replace_bracket, sql)

        return converted

    def _convert_date_functions(self, sql: str) -> str:
        """
        Convert T-SQL date functions to Databricks SQL equivalents.

        Args:
            sql: SQL string with T-SQL date functions

        Returns:
            SQL with Databricks date functions
        """
        converted = sql

        # Apply date function patterns
        for pattern, replacement in DATE_FUNCTION_PATTERNS.items():
            if re.search(pattern, converted, re.IGNORECASE):
                converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
                self._log_conversion(f'Converted date function: {pattern} -> {replacement}')

        # Handle GETDATE() -> CURRENT_DATE() or CURRENT_TIMESTAMP()
        # Context matters: if CAST AS DATE, use CURRENT_DATE()
        converted = re.sub(
            r'CAST\s*\(\s*GETDATE\(\)\s+AS\s+DATE\s*\)',
            'CURRENT_DATE()',
            converted,
            flags=re.IGNORECASE
        )

        # General GETDATE() -> CURRENT_TIMESTAMP()
        converted = re.sub(r'\bGETDATE\(\)', 'CURRENT_TIMESTAMP()', converted, flags=re.IGNORECASE)
        converted = re.sub(r'\bGETUTCDATE\(\)', 'CURRENT_TIMESTAMP()', converted, flags=re.IGNORECASE)
        converted = re.sub(r'\bSYSDATETIME\(\)', 'CURRENT_TIMESTAMP()', converted, flags=re.IGNORECASE)

        return converted

    def _convert_system_functions(self, sql: str) -> str:
        """
        Convert T-SQL system functions to Databricks equivalents.

        Args:
            sql: SQL string with T-SQL functions

        Returns:
            SQL with Databricks functions
        """
        converted = sql

        # Apply function mappings
        for tsql_func, databricks_func in TSQL_FUNCTION_MAPPINGS.items():
            pattern = r'\b' + tsql_func.replace('(', r'\(').replace(')', r'\)') + r'\b'
            if re.search(pattern, converted, re.IGNORECASE):
                converted = re.sub(pattern, databricks_func, converted, flags=re.IGNORECASE)
                self._log_conversion(f'Converted function: {tsql_func} -> {databricks_func}')

        return converted

    def _convert_table_references(self, sql: str) -> str:
        """
        Convert table references from [dbo].[table] to catalog.schema.table format.

        Args:
            sql: SQL string with table references

        Returns:
            SQL with Unity Catalog references
        """
        converted = sql

        # If catalog and schema specified, use Unity Catalog format
        if self.catalog and self.schema:
            # Replace dbo.table or [dbo].[table] patterns
            pattern = r'(?:\[?dbo\]?\.)?(?:\[?(\w+)\]?)'

            def replace_table_ref(match):
                table_name = match.group(1)
                if table_name and table_name.upper() not in ['SELECT', 'FROM', 'JOIN', 'WHERE', 'GROUP', 'ORDER']:
                    self._log_conversion(f'Converted table reference to Unity Catalog format: {self.catalog}.{self.schema}.{table_name}')
                    return f'{self.catalog}.{self.schema}.{table_name}'
                return match.group(0)

            # This is simplified - production would need more sophisticated parsing
            # converted = re.sub(pattern, replace_table_ref, converted)

        return converted

    def _convert_data_types(self, ddl: str) -> str:
        """
        Convert SQL Server data types to Databricks types in DDL.

        Args:
            ddl: DDL string with SQL Server types

        Returns:
            DDL with Databricks types
        """
        converted = ddl

        # Pattern to match column definitions: column_name TYPE(params) [NOT NULL] [DEFAULT...]
        pattern = r'(\w+)\s+([A-Z]+(?:\([^\)]+\))?)'

        def replace_type(match):
            col_name = match.group(1)
            sql_type = match.group(2)
            databricks_type = map_sql_server_type(sql_type)

            if databricks_type != sql_type:
                self._log_conversion(f'Converted column {col_name} type: {sql_type} -> {databricks_type}')

            return f'{col_name} {databricks_type}'

        # Apply type conversions
        lines = converted.split('\n')
        for i, line in enumerate(lines):
            if re.search(pattern, line) and 'CREATE TABLE' not in line.upper():
                lines[i] = re.sub(pattern, replace_type, line)

        converted = '\n'.join(lines)

        return converted

    def _convert_constraints(self, ddl: str) -> str:
        """
        Convert SQL Server constraints to Databricks equivalents.

        Note: Databricks has limited constraint support. PRIMARY KEY and NOT NULL
        are supported, but FOREIGN KEY and CHECK constraints are informational only.

        Args:
            ddl: DDL string with constraints

        Returns:
            DDL with Databricks-compatible constraints
        """
        converted = ddl

        # PRIMARY KEY is supported
        if 'PRIMARY KEY' in converted.upper():
            self._log_conversion('PRIMARY KEY constraint preserved (supported in Databricks)')

        # FOREIGN KEY is informational only
        if 'FOREIGN KEY' in converted.upper():
            self._log_conversion('WARNING: FOREIGN KEY constraints are informational only in Databricks')

        # CHECK constraints are informational
        if re.search(r'CHECK\s*\(', converted, re.IGNORECASE):
            self._log_conversion('WARNING: CHECK constraints are informational only in Databricks')

        return converted

    def _convert_default_values(self, ddl: str) -> str:
        """
        Convert DEFAULT value syntax.

        Args:
            ddl: DDL string with DEFAULT clauses

        Returns:
            DDL with Databricks DEFAULT syntax
        """
        converted = ddl

        # NEWID() -> UUID()
        converted = re.sub(r'DEFAULT\s+NEWID\(\)', 'DEFAULT UUID()', converted, flags=re.IGNORECASE)

        # GETDATE() -> CURRENT_TIMESTAMP()
        converted = re.sub(r'DEFAULT\s+GETDATE\(\)', 'DEFAULT CURRENT_TIMESTAMP()', converted, flags=re.IGNORECASE)

        return converted

    def _clean_format(self, sql: str) -> str:
        """
        Clean up SQL formatting and remove T-SQL specific artifacts.

        Args:
            sql: SQL string

        Returns:
            Cleaned SQL
        """
        # Remove #(lf) line feed characters (Power Query artifacts)
        sql = sql.replace('#(lf)', '\n')

        # Remove extra whitespace
        sql = re.sub(r'\s+', ' ', sql)
        sql = re.sub(r'\s*,\s*', ', ', sql)
        sql = re.sub(r'\s*\(\s*', '(', sql)
        sql = re.sub(r'\s*\)\s*', ') ', sql)

        return sql.strip()

    def _log_conversion(self, message: str):
        """Log a conversion step."""
        self.conversion_log.append({
            'message': message,
            'timestamp': None  # Could add datetime if needed
        })
        logger.info(message)
