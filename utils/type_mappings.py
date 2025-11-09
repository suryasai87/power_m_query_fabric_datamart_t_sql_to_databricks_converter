"""
Data type mappings from SQL Server / T-SQL / Fabric to Databricks SQL.

This module provides comprehensive type mapping dictionaries for converting
SQL Server data types to their Databricks SQL equivalents.
"""

# SQL Server to Databricks type mappings
SQL_SERVER_TO_DATABRICKS_TYPES = {
    # Exact numerics
    'BIGINT': 'BIGINT',
    'INT': 'INT',
    'SMALLINT': 'SMALLINT',
    'TINYINT': 'TINYINT',
    'BIT': 'BOOLEAN',
    'DECIMAL': 'DECIMAL',
    'NUMERIC': 'DECIMAL',
    'MONEY': 'DECIMAL(19,4)',
    'SMALLMONEY': 'DECIMAL(10,4)',

    # Approximate numerics
    'FLOAT': 'DOUBLE',
    'REAL': 'FLOAT',

    # Date and time
    'DATE': 'DATE',
    'DATETIME': 'TIMESTAMP',
    'DATETIME2': 'TIMESTAMP',
    'SMALLDATETIME': 'TIMESTAMP',
    'TIME': 'STRING',  # Databricks doesn't have native TIME type
    'DATETIMEOFFSET': 'TIMESTAMP',

    # Character strings
    'CHAR': 'STRING',
    'VARCHAR': 'STRING',
    'TEXT': 'STRING',
    'NCHAR': 'STRING',
    'NVARCHAR': 'STRING',
    'NTEXT': 'STRING',

    # Binary
    'BINARY': 'BINARY',
    'VARBINARY': 'BINARY',
    'IMAGE': 'BINARY',

    # Other
    'UNIQUEIDENTIFIER': 'STRING',
    'XML': 'STRING',
    'JSON': 'STRING',
}


def map_sql_server_type(sql_type: str) -> str:
    """
    Map SQL Server data type to Databricks SQL equivalent.

    Args:
        sql_type: SQL Server data type (e.g., 'INT', 'VARCHAR(100)', 'DECIMAL(10,2)')

    Returns:
        Databricks SQL data type

    Examples:
        >>> map_sql_server_type('INT')
        'INT'
        >>> map_sql_server_type('VARCHAR(100)')
        'STRING'
        >>> map_sql_server_type('DECIMAL(10,2)')
        'DECIMAL(10,2)'
    """
    import re

    # Extract base type and parameters
    match = re.match(r'([A-Z]+)(\(.*\))?', sql_type.upper())
    if not match:
        return 'STRING'  # Default fallback

    base_type = match.group(1)
    params = match.group(2) if match.group(2) else ''

    # Get Databricks type
    if base_type in SQL_SERVER_TO_DATABRICKS_TYPES:
        databricks_type = SQL_SERVER_TO_DATABRICKS_TYPES[base_type]

        # If Databricks type doesn't have params but SQL type does, handle it
        if params and databricks_type in ['DECIMAL', 'NUMERIC']:
            return f"{databricks_type}{params}"
        elif params and databricks_type == 'STRING':
            # STRING doesn't need length parameter in Databricks
            return 'STRING'
        else:
            return databricks_type
    else:
        return 'STRING'  # Default to STRING for unknown types


# T-SQL function mappings to Databricks SQL
TSQL_FUNCTION_MAPPINGS = {
    'GETDATE()': 'CURRENT_TIMESTAMP()',
    'GETUTCDATE()': 'CURRENT_TIMESTAMP()',
    'SYSDATETIME()': 'CURRENT_TIMESTAMP()',
    'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP()',
    'NEWID()': 'UUID()',
    'ISNULL': 'COALESCE',
    'LEN': 'LENGTH',
    'CHARINDEX': 'INSTR',
    'STUFF': 'OVERLAY',
    'DATEPART': 'EXTRACT',
}


# Date function conversion patterns
DATE_FUNCTION_PATTERNS = {
    r'DATEADD\s*\(\s*day\s*,\s*(-?\d+)\s*,\s*GETDATE\(\)\s*\)': r'DATE_ADD(CURRENT_DATE(), \1)',
    r'DATEADD\s*\(\s*month\s*,\s*(-?\d+)\s*,\s*GETDATE\(\)\s*\)': r'ADD_MONTHS(CURRENT_DATE(), \1)',
    r'DATEADD\s*\(\s*year\s*,\s*(-?\d+)\s*,\s*GETDATE\(\)\s*\)': r'ADD_MONTHS(CURRENT_DATE(), \1 * 12)',
    r'DATEADD\s*\(\s*day\s*,\s*(-?\d+)\s*,\s*([^\)]+)\s*\)': r'DATE_ADD(\2, \1)',
    r'DATEDIFF\s*\(\s*day\s*,\s*([^,]+)\s*,\s*([^\)]+)\s*\)': r'DATEDIFF(\2, \1)',
    r'CAST\s*\(\s*([^\s]+)\s+AS\s+DATE\s*\)': r'DATE(\1)',
}


# Power M Query to Databricks SQL function mappings
POWER_M_FUNCTION_MAPPINGS = {
    'Date.IsInPreviousNDays': lambda days: f'CURRENT_DATE() - INTERVAL {days} DAYS',
    'Date.IsInPreviousNWeeks': lambda weeks: f'CURRENT_DATE() - INTERVAL {weeks * 7} DAYS',
    'Date.IsInPreviousNMonths': lambda months: f'CURRENT_DATE() - INTERVAL {months} MONTHS',
    'Date.IsInPreviousNYears': lambda years: f'CURRENT_DATE() - INTERVAL {years} YEARS',
    'DateTime.LocalNow()': 'CURRENT_TIMESTAMP()',
    'DateTime.FixedLocalNow()': 'CURRENT_TIMESTAMP()',
}
