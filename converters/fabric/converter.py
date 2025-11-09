"""
Microsoft Fabric Datamart to Databricks SQL Converter.

Fabric Datamarts use T-SQL syntax, so this module leverages the T-SQL converter.
"""

from converters.tsql.converter import TSQLConverter


class FabricConverter(TSQLConverter):
    """
    Converter for Microsoft Fabric Datamart SQL to Databricks SQL.

    Fabric Datamarts use T-SQL dialect, so we inherit from TSQLConverter.
    Additional Fabric-specific transformations can be added here.
    """

    def __init__(self, catalog: str = None, schema: str = None):
        """
        Initialize Fabric converter.

        Args:
            catalog: Target Databricks Unity Catalog name
            schema: Target schema name
        """
        super().__init__(catalog, schema)
        self._log_conversion('Using Fabric Datamart converter (T-SQL dialect)')

    def convert_fabric_specific_features(self, sql: str) -> str:
        """
        Convert Fabric-specific features that differ from standard T-SQL.

        Args:
            sql: SQL string with Fabric-specific syntax

        Returns:
            SQL with Databricks equivalents
        """
        # Fabric-specific transformations can be added here
        # For now, Fabric uses standard T-SQL

        return sql
