"""
Data type mappings from SQL Server / T-SQL / Fabric to Databricks SQL.

Comprehensive type mapping dictionaries and function conversion tables
for converting SQL Server data types and functions to Databricks SQL equivalents.
"""

import re

# ------------------------------------------------------------------ #
#  SQL Server -> Databricks type mappings                             #
# ------------------------------------------------------------------ #

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
    'TIME': 'STRING',  # Databricks has no native TIME type
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

    # Spatial / CLR types (no Databricks equivalent - map to STRING)
    'GEOGRAPHY': 'STRING',
    'GEOMETRY': 'STRING',
    'HIERARCHYID': 'STRING',

    # Special types
    'SQL_VARIANT': 'STRING',
    'ROWVERSION': 'BINARY',
    'TIMESTAMP': 'BINARY',  # SQL Server TIMESTAMP is a row-version, not a datetime
}

# Types that deserve a warning when converted
TYPES_WITH_WARNINGS = {
    'GEOGRAPHY': 'GEOGRAPHY has no Databricks equivalent; stored as WKT STRING. Spatial functions will not work.',
    'GEOMETRY': 'GEOMETRY has no Databricks equivalent; stored as WKT STRING. Spatial functions will not work.',
    'HIERARCHYID': 'HIERARCHYID converted to STRING. Hierarchy traversal functions will not work.',
    'SQL_VARIANT': 'SQL_VARIANT converted to STRING. Type introspection lost.',
    'ROWVERSION': 'ROWVERSION/TIMESTAMP converted to BINARY. Auto-increment semantics lost.',
    'IMAGE': 'IMAGE is deprecated in SQL Server; converted to BINARY.',
}


def map_sql_server_type(sql_type: str) -> str:
    """
    Map a SQL Server data type to its Databricks SQL equivalent.

    Args:
        sql_type: SQL Server data type (e.g., 'INT', 'VARCHAR(100)', 'DECIMAL(10,2)')

    Returns:
        Databricks SQL data type string

    Examples:
        >>> map_sql_server_type('INT')
        'INT'
        >>> map_sql_server_type('VARCHAR(100)')
        'STRING'
        >>> map_sql_server_type('DECIMAL(10,2)')
        'DECIMAL(10,2)'
        >>> map_sql_server_type('GEOGRAPHY')
        'STRING'
    """
    match = re.match(r'([A-Z_]+)(\(.*\))?', sql_type.upper().strip())
    if not match:
        return 'STRING'  # Default fallback

    base_type = match.group(1)
    params = match.group(2) or ''

    if base_type in SQL_SERVER_TO_DATABRICKS_TYPES:
        databricks_type = SQL_SERVER_TO_DATABRICKS_TYPES[base_type]

        # Preserve precision/scale for DECIMAL
        if params and databricks_type in ('DECIMAL',):
            return f'{databricks_type}{params}'
        # STRING doesn't need length params
        if databricks_type == 'STRING':
            return 'STRING'
        return databricks_type

    return 'STRING'


# ------------------------------------------------------------------ #
#  T-SQL -> Databricks function mappings                              #
# ------------------------------------------------------------------ #

TSQL_FUNCTION_MAPPINGS = {
    # Date/time
    'GETDATE()': 'CURRENT_TIMESTAMP()',
    'GETUTCDATE()': 'CURRENT_TIMESTAMP()',
    'SYSDATETIME()': 'CURRENT_TIMESTAMP()',
    'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP()',
    'NEWID()': 'UUID()',

    # Null handling
    'ISNULL': 'COALESCE',

    # String functions
    'LEN': 'LENGTH',
    'DATALENGTH': 'OCTET_LENGTH',
    'REPLICATE': 'REPEAT',
    'SPACE': 'REPEAT',  # SPACE(n) -> REPEAT(' ', n) needs special handling
    'QUOTENAME': 'CONCAT',  # Approximate; needs manual review

    # Math
    'SQUARE': 'POWER',  # SQUARE(x) -> POWER(x, 2) needs special handling
    'LOG10': 'LOG10',
    'ATN2': 'ATAN2',

    # Type conversion
    'CONVERT': 'CAST',  # Approximate; CONVERT has style codes
    'TRY_CONVERT': 'TRY_CAST',

    # Misc
    'STRING_AGG': 'CONCAT_WS',  # Approximate; argument order differs
    'IIF': 'IF',
}

# CHARINDEX(substring, string) -> LOCATE(substring, string)
# Note: sqlglot handles this automatically, but for regex fallback:
CHARINDEX_PATTERN = r'CHARINDEX\s*\(\s*([^,]+),\s*([^)]+)\)'
CHARINDEX_REPLACEMENT = r'LOCATE(\1, \2)'

# STUFF(string, start, length, replacement) -> OVERLAY(string PLACING replacement FROM start FOR length)
STUFF_PATTERN = r'STUFF\s*\(\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^)]+)\)'
STUFF_REPLACEMENT = r'OVERLAY(\1 PLACING \4 FROM \2 FOR \3)'

# DATEPART(part, date) -> EXTRACT(part FROM date)
DATEPART_PATTERN = r'DATEPART\s*\(\s*(\w+)\s*,\s*([^)]+)\)'
DATEPART_REPLACEMENT = r'EXTRACT(\1 FROM \2)'

# TOP N handling (for regex fallback)
TOP_PATTERN = r'\bSELECT\s+TOP\s+\(?(\d+)\)?\b'


# ------------------------------------------------------------------ #
#  Date function conversion patterns                                  #
# ------------------------------------------------------------------ #

DATE_FUNCTION_PATTERNS = {
    # DATEADD variants
    r'DATEADD\s*\(\s*day\s*,\s*(-?\d+)\s*,\s*GETDATE\(\)\s*\)':
        r'DATE_ADD(CURRENT_DATE(), \1)',
    r'DATEADD\s*\(\s*month\s*,\s*(-?\d+)\s*,\s*GETDATE\(\)\s*\)':
        r'ADD_MONTHS(CURRENT_DATE(), \1)',
    r'DATEADD\s*\(\s*year\s*,\s*(-?\d+)\s*,\s*GETDATE\(\)\s*\)':
        r'ADD_MONTHS(CURRENT_DATE(), \1 * 12)',
    r'DATEADD\s*\(\s*day\s*,\s*(-?\d+)\s*,\s*([^\)]+)\s*\)':
        r'DATE_ADD(\2, \1)',

    # DATEDIFF (note: Databricks DATEDIFF takes (end, start) not (part, start, end))
    r'DATEDIFF\s*\(\s*day\s*,\s*([^,]+)\s*,\s*([^\)]+)\s*\)':
        r'DATEDIFF(\2, \1)',

    # CAST AS DATE
    r'CAST\s*\(\s*([^\s]+)\s+AS\s+DATE\s*\)':
        r'DATE(\1)',

    # DATEPART -> EXTRACT
    r'DATEPART\s*\(\s*(\w+)\s*,\s*([^)]+)\)':
        r'EXTRACT(\1 FROM \2)',

    # EOMONTH -> LAST_DAY
    r'EOMONTH\s*\(\s*([^)]+)\s*\)':
        r'LAST_DAY(\1)',

    # ISDATE -> TRY_CAST ... IS NOT NULL
    r'ISDATE\s*\(\s*([^)]+)\s*\)':
        r'TRY_CAST(\1 AS DATE) IS NOT NULL',
}


# ------------------------------------------------------------------ #
#  Power M -> Databricks SQL function mappings                        #
# ------------------------------------------------------------------ #

POWER_M_FUNCTION_MAPPINGS = {
    'Date.IsInPreviousNDays': lambda days: f'CURRENT_DATE() - INTERVAL {days} DAYS',
    'Date.IsInPreviousNWeeks': lambda weeks: f'CURRENT_DATE() - INTERVAL {weeks * 7} DAYS',
    'Date.IsInPreviousNMonths': lambda months: f'CURRENT_DATE() - INTERVAL {months} MONTHS',
    'Date.IsInPreviousNYears': lambda years: f'CURRENT_DATE() - INTERVAL {years} YEARS',
    'DateTime.LocalNow()': 'CURRENT_TIMESTAMP()',
    'DateTime.FixedLocalNow()': 'CURRENT_TIMESTAMP()',
}
