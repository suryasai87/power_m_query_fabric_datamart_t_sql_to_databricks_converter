"""
Power Query M to Databricks SQL Converter.

This module converts Power Query M language scripts to equivalent Databricks SQL queries.
"""

import re
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class PowerMConverter:
    """
    Converter for Power Query M to Databricks SQL.

    Note: This handles common patterns. Complex M queries may require manual review.
    """

    def __init__(self, catalog: str = None, schema: str = None):
        """
        Initialize Power M converter.

        Args:
            catalog: Target Databricks Unity Catalog name
            schema: Target schema name
        """
        self.catalog = catalog
        self.schema = schema
        self.conversion_log = []

    def convert(self, power_m_query: str) -> Tuple[str, List[Dict]]:
        """
        Convert Power Query M to Databricks SQL.

        Args:
            power_m_query: Power Query M script

        Returns:
            Tuple of (databricks_sql, conversion_notes)
        """
        self.conversion_log = []

        # Extract key information from M query
        source_info = self._extract_source(power_m_query)
        table_name = self._extract_table_name(power_m_query)
        selected_columns = self._extract_selected_columns(power_m_query)
        date_filter = self._extract_date_filter(power_m_query)
        sort_order = self._extract_sort_order(power_m_query)

        # Build Databricks SQL query
        databricks_sql = self._build_databricks_query(
            source_info,
            table_name,
            selected_columns,
            date_filter,
            sort_order
        )

        return databricks_sql, self.conversion_log

    def _extract_source(self, m_query: str) -> Dict:
        """Extract source information from M query."""
        source_info = {}

        # Check for Salesforce source
        if 'Salesforce.Data' in m_query:
            match = re.search(r'Salesforce\.Data\("([^"]+)"', m_query)
            if match:
                source_info['type'] = 'Salesforce'
                source_info['url'] = match.group(1)
                self._log_conversion('Detected Salesforce data source')

        # Check for SQL Server source
        elif 'Sql.Database' in m_query or 'Sql.Databases' in m_query:
            source_info['type'] = 'SQLServer'
            self._log_conversion('Detected SQL Server data source')

        # Default to unknown
        else:
            source_info['type'] = 'Unknown'
            self._log_conversion('WARNING: Could not detect data source type')

        return source_info

    def _extract_table_name(self, m_query: str) -> str:
        """Extract table name from M query."""
        # Look for [Name="TableName"] pattern
        match = re.search(r'\[Name="([^"]+)"\]', m_query)
        if match:
            table_name = match.group(1)
            self._log_conversion(f'Extracted table name: {table_name}')
            return table_name.lower().replace(' ', '_')

        # Look for Source{[Name="TableName"]}[Data] pattern (common in Salesforce)
        match = re.search(r'Source\{(\[Name="([^"]+)"\])\}\[Data\]', m_query)
        if match:
            table_name = match.group(2)
            self._log_conversion(f'Extracted Salesforce object name: {table_name}')
            return table_name.lower().replace(' ', '_')

        return 'unknown_table'

    def _extract_selected_columns(self, m_query: str) -> List[str]:
        """Extract column selection from M query."""
        # Look for Table.SelectColumns pattern
        match = re.search(r'Table\.SelectColumns\([^,]+,\s*\{([^\}]+)\}', m_query)
        if match:
            columns_str = match.group(1)
            # Extract column names from quoted strings
            columns = re.findall(r'"([^"]+)"', columns_str)
            self._log_conversion(f'Found {len(columns)} selected columns')
            return columns

        # If no SelectColumns, assume SELECT *
        self._log_conversion('No column selection found, using SELECT *')
        return ['*']

    def _extract_date_filter(self, m_query: str) -> str:
        """Extract date filtering logic from M query."""
        # Look for Date.IsInPreviousNDays pattern
        match = re.search(r'Date\.IsInPreviousNDays\(\[([^\]]+)\],\s*(\d+)\)', m_query)
        if match:
            date_column = match.group(1)
            days = match.group(2)
            # Convert to 365 days = 12 months
            if days == '365':
                months = 12
                self._log_conversion(f'Converted Date.IsInPreviousNDays({days}) to 12 months filter')
                return f'{date_column} >= CURRENT_DATE() - INTERVAL {months} MONTHS'
            else:
                self._log_conversion(f'Converted Date.IsInPreviousNDays({days}) to days filter')
                return f'{date_column} >= CURRENT_DATE() - INTERVAL {days} DAYS'

        # Look for Date.IsInPreviousNMonths
        match = re.search(r'Date\.IsInPreviousNMonths\(\[([^\]]+)\],\s*(\d+)\)', m_query)
        if match:
            date_column = match.group(1)
            months = match.group(2)
            self._log_conversion(f'Converted Date.IsInPreviousNMonths({months})')
            return f'{date_column} >= CURRENT_DATE() - INTERVAL {months} MONTHS'

        return None

    def _extract_sort_order(self, m_query: str) -> str:
        """Extract sort order from M query."""
        # Look for Table.Sort pattern
        match = re.search(r'Table\.Sort\([^,]+,\s*\{\{"([^"]+)",\s*Order\.(\w+)\}\}', m_query)
        if match:
            sort_column = match.group(1)
            order = match.group(2)  # Ascending or Descending
            order_sql = 'DESC' if order == 'Descending' else 'ASC'
            self._log_conversion(f'Found sort order: {sort_column} {order_sql}')
            return f'{sort_column} {order_sql}'

        return None

    def _build_databricks_query(
        self,
        source_info: Dict,
        table_name: str,
        selected_columns: List[str],
        date_filter: str,
        sort_order: str
    ) -> str:
        """Build Databricks SQL query from extracted components."""
        # Build column list
        if selected_columns == ['*']:
            columns_sql = '*'
        else:
            columns_sql = ',\n  '.join(selected_columns)

        # Build FROM clause with catalog/schema if specified
        if self.catalog and self.schema:
            from_clause = f'{self.catalog}.{self.schema}.{table_name}'
        else:
            from_clause = table_name

        # Build WHERE clause
        where_clause = f'\nWHERE {date_filter}' if date_filter else ''

        # Build ORDER BY clause
        order_clause = f'\nORDER BY {sort_order}' if sort_order else ''

        # Assemble query
        query = f"""-- Converted from Power Query M
-- Source: {source_info.get('type', 'Unknown')}
-- Target Table: {table_name}

CREATE OR REPLACE TABLE {table_name} AS
SELECT
  {columns_sql}
FROM {from_clause}{where_clause}{order_clause};"""

        self._log_conversion('Generated Databricks SQL CREATE TABLE statement')

        return query

    def _log_conversion(self, message: str):
        """Log a conversion step."""
        self.conversion_log.append({
            'message': message,
            'timestamp': None
        })
        logger.info(message)
