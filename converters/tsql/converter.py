"""T-SQL to Databricks SQL converter using sqlglot AST transpilation."""

try:
    import sqlglot
    from sqlglot import transpile
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    sqlglot = None
    transpile = None

import re
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class TSQLConverter:
    """Converter for T-SQL to Databricks SQL using sqlglot AST transpilation."""

    def __init__(self, target_catalog: str = "main", target_schema: str = "default",
                 catalog: str = None, schema: str = None):
        """
        Initialize T-SQL converter.

        Args:
            target_catalog: Target Databricks Unity Catalog name
            target_schema: Target schema name
            catalog: Alias for target_catalog (backward compat)
            schema: Alias for target_schema (backward compat)
        """
        self.target_catalog = catalog or target_catalog
        self.target_schema = schema or target_schema
        self.conversion_log = []

    # ------------------------------------------------------------------ #
    #  Public API                                                         #
    # ------------------------------------------------------------------ #

    def convert(self, sql_text: str) -> dict:
        """Convert T-SQL to Databricks SQL using sqlglot AST transpilation."""
        warnings = []

        preprocessed = self._preprocess(sql_text)

        if SQLGLOT_AVAILABLE:
            try:
                results = transpile(
                    preprocessed,
                    read="tsql",
                    write="databricks",
                    pretty=True,
                )
                converted = ";\n\n".join(results)
            except Exception as e:
                logger.warning(f"sqlglot parse failed, falling back to regex: {e}")
                warnings.append(f"AST parsing failed, used regex fallback: {str(e)}")
                converted = self._regex_fallback(preprocessed)
        else:
            warnings.append("sqlglot not installed, using regex fallback")
            converted = self._regex_fallback(preprocessed)

        converted = self._rewrite_table_refs(converted)

        return {
            "converted_sql": converted,
            "warnings": warnings,
            "method": "sqlglot_ast" if not warnings else "regex_fallback",
        }

    def convert_query(self, tsql_query: str) -> Tuple[str, List[Dict]]:
        """
        Convert T-SQL query to Databricks SQL (legacy API).

        Returns:
            Tuple of (converted_query, conversion_notes)
        """
        result = self.convert(tsql_query)
        notes = [{"message": w, "timestamp": None} for w in result.get("warnings", [])]
        return result["converted_sql"], notes

    def convert_ddl(self, ddl_text: str) -> dict:
        """Convert T-SQL DDL to Databricks DDL."""
        warnings = []
        if SQLGLOT_AVAILABLE:
            try:
                results = transpile(ddl_text, read="tsql", write="databricks", pretty=True)
                converted = ";\n\n".join(results)
                if "CREATE TABLE" in converted.upper() and "USING DELTA" not in converted.upper():
                    converted = re.sub(
                        r'(CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+\S+\s*\([^)]+\))',
                        r'\1\nUSING DELTA',
                        converted,
                        flags=re.IGNORECASE | re.DOTALL,
                    )
            except Exception as e:
                warnings.append(f"DDL parse error: {str(e)}")
                converted = self._regex_ddl_fallback(ddl_text)
        else:
            warnings.append("sqlglot not installed, using regex DDL fallback")
            converted = self._regex_ddl_fallback(ddl_text)

        converted = self._rewrite_table_refs(converted)
        return {"converted_sql": converted, "warnings": warnings}

    # ------------------------------------------------------------------ #
    #  Pre / post processing                                              #
    # ------------------------------------------------------------------ #

    def _preprocess(self, sql_text: str) -> str:
        """Handle constructs that sqlglot may struggle with."""
        text = sql_text
        # Remove SET NOCOUNT ON/OFF
        text = re.sub(r'SET\s+NOCOUNT\s+(ON|OFF)\s*;?', '', text, flags=re.IGNORECASE)
        # Remove WITH (NOLOCK) and other table hints
        text = re.sub(r'\bWITH\s*\(\s*NOLOCK\s*\)', '', text, flags=re.IGNORECASE)
        # Remove GO statements
        text = re.sub(r'^\s*GO\s*$', '', text, flags=re.MULTILINE | re.IGNORECASE)
        return text.strip()

    def _rewrite_table_refs(self, sql_text: str) -> str:
        """Rewrite dbo.table references to catalog.schema.table."""
        text = sql_text
        # `dbo`.`TableName` -> catalog.schema.TableName  (backtick-quoted form from sqlglot)
        text = re.sub(
            r'`dbo`\.`(\w+)`',
            f'{self.target_catalog}.{self.target_schema}.`\\1`',
            text,
        )
        # dbo.TableName -> catalog.schema.TableName  (unquoted form)
        text = re.sub(
            r'\bdbo\.(\w+)',
            f'{self.target_catalog}.{self.target_schema}.\\1',
            text,
        )
        return text

    # ------------------------------------------------------------------ #
    #  Regex fallbacks                                                    #
    # ------------------------------------------------------------------ #

    def _regex_fallback(self, sql_text: str) -> str:
        """Regex-based fallback when AST parsing fails."""
        converted = sql_text
        # Bracket identifiers -> backticks
        converted = re.sub(r'\[([^\]]+)\]', r'`\1`', converted)
        # GETDATE() -> CURRENT_TIMESTAMP()
        converted = re.sub(r'\bGETDATE\s*\(\s*\)', 'CURRENT_TIMESTAMP()', converted, flags=re.IGNORECASE)
        converted = re.sub(r'\bGETUTCDATE\s*\(\s*\)', 'CURRENT_TIMESTAMP()', converted, flags=re.IGNORECASE)
        converted = re.sub(r'\bSYSDATETIME\s*\(\s*\)', 'CURRENT_TIMESTAMP()', converted, flags=re.IGNORECASE)
        # ISNULL -> COALESCE
        converted = re.sub(r'\bISNULL\s*\(', 'COALESCE(', converted, flags=re.IGNORECASE)
        # LEN -> LENGTH
        converted = re.sub(r'\bLEN\s*\(', 'LENGTH(', converted, flags=re.IGNORECASE)
        # NEWID() -> UUID()
        converted = re.sub(r'\bNEWID\s*\(\s*\)', 'UUID()', converted, flags=re.IGNORECASE)
        # TOP N -> LIMIT N (simple cases)
        top_match = re.search(r'\bTOP\s+(\d+)\b', sql_text, re.IGNORECASE)
        converted = re.sub(r'\bSELECT\s+TOP\s+(\d+)\b', 'SELECT', converted, flags=re.IGNORECASE)
        if top_match:
            converted = converted.rstrip().rstrip(';')
            converted += f"\nLIMIT {top_match.group(1)}"
        return converted

    def _regex_ddl_fallback(self, ddl_text: str) -> str:
        """Regex-based DDL fallback."""
        from utils.type_mappings import SQL_SERVER_TO_DATABRICKS_TYPES

        converted = ddl_text
        converted = re.sub(r'\[([^\]]+)\]', r'`\1`', converted)
        for tsql_type, db_type in SQL_SERVER_TO_DATABRICKS_TYPES.items():
            converted = re.sub(rf'\b{tsql_type}\b', db_type, converted, flags=re.IGNORECASE)
        if "CREATE TABLE" in converted.upper() and "USING DELTA" not in converted.upper():
            converted = converted.rstrip().rstrip(';') + "\nUSING DELTA;"
        return converted

    # ------------------------------------------------------------------ #
    #  Logging helper                                                     #
    # ------------------------------------------------------------------ #

    def _log_conversion(self, message: str):
        """Log a conversion step."""
        self.conversion_log.append({
            'message': message,
            'timestamp': None,
        })
        logger.info(message)
