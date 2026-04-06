"""Fabric Datamart to Databricks converter."""

from converters.tsql.converter import TSQLConverter
import re
import logging

logger = logging.getLogger(__name__)


class FabricConverter(TSQLConverter):
    """Extends T-SQL converter with Fabric Datamart-specific handling."""

    def __init__(self, target_catalog: str = "main", target_schema: str = "default",
                 catalog: str = None, schema: str = None):
        super().__init__(
            target_catalog=target_catalog,
            target_schema=target_schema,
            catalog=catalog,
            schema=schema,
        )

    def convert(self, sql_text: str) -> dict:
        """Convert Fabric Datamart SQL to Databricks SQL."""
        processed = self._convert_fabric_features(sql_text)
        result = super().convert(processed)
        # Post-process: remove any TBLPROPERTIES with DISTRIBUTION that sqlglot generated
        result["converted_sql"] = self._strip_distribution_tblproperties(
            result["converted_sql"]
        )
        result["source_type"] = "fabric_datamart"
        return result

    def convert_query(self, tsql_query: str):
        """Legacy API: convert Fabric query and return (sql, notes) tuple."""
        result = self.convert(tsql_query)
        notes = [{"message": w, "timestamp": None} for w in result.get("warnings", [])]
        return result["converted_sql"], notes

    def _convert_fabric_features(self, sql_text: str) -> str:
        """Handle Fabric-specific SQL extensions."""
        text = sql_text
        # Remove Fabric-specific OPTION hints (e.g., OPTION (LABEL = '...'))
        text = re.sub(
            r'\bOPTION\s*\([^)]*LABEL\s*=\s*[^)]*\)',
            '',
            text,
            flags=re.IGNORECASE,
        )
        # EXTERNAL TABLE -> managed Delta table
        text = re.sub(
            r'\bCREATE\s+EXTERNAL\s+TABLE\b',
            'CREATE TABLE',
            text,
            flags=re.IGNORECASE,
        )
        # Remove CLUSTERED COLUMNSTORE INDEX (before WITH clause to simplify)
        text = re.sub(
            r',?\s*CLUSTERED\s+COLUMNSTORE\s+INDEX',
            '',
            text,
            flags=re.IGNORECASE,
        )
        # Remove entire WITH (...) clause containing DISTRIBUTION
        text = re.sub(
            r'\bWITH\s*\([^)]*DISTRIBUTION[^)]*\)',
            '',
            text,
            flags=re.IGNORECASE,
        )
        return text

    def _strip_distribution_tblproperties(self, sql_text: str) -> str:
        """Remove TBLPROPERTIES blocks containing DISTRIBUTION (sqlglot artifact)."""
        text = re.sub(
            r'\s*TBLPROPERTIES\s*\([^)]*DISTRIBUTION[^)]*\)',
            '',
            sql_text,
            flags=re.IGNORECASE,
        )
        return text
