"""
Power Query M to Databricks SQL Converter.

Converts Power Query M language scripts to equivalent Databricks SQL queries.
Uses regex-based extraction of M constructs since M is not a SQL dialect.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class PowerMConverter:
    """
    Converter for Power Query M to Databricks SQL.

    Handles common M patterns: Table.SelectColumns, Table.SelectRows,
    Table.Sort, Table.AddColumn, Table.Group, Table.Join,
    Table.RenameColumns, Table.Distinct, Table.FirstN, Table.Skip, etc.

    Complex M queries may require manual review.
    """

    def __init__(self, target_catalog: str = "main", target_schema: str = "default",
                 catalog: str = None, schema: str = None):
        self.target_catalog = catalog or target_catalog
        self.target_schema = schema or target_schema
        self.conversion_log: List[Dict] = []

    # ------------------------------------------------------------------ #
    #  Public API                                                         #
    # ------------------------------------------------------------------ #

    def convert(self, power_m_query: str) -> dict:
        """
        Convert Power Query M to Databricks SQL.

        Returns:
            dict with keys: converted_sql, warnings, conversion_log
        """
        self.conversion_log = []
        warnings: List[str] = []

        # Parse let/in variable chain
        steps = self._parse_let_in(power_m_query)

        # Extract components from all steps
        source_info = self._extract_source(power_m_query)
        table_name = self._extract_table_name(power_m_query)
        selected_columns = self._extract_selected_columns(power_m_query)
        renamed_columns = self._extract_renamed_columns(power_m_query)
        added_columns = self._extract_added_columns(power_m_query, warnings)
        filters = self._extract_filters(power_m_query)
        date_filter = self._extract_date_filter(power_m_query)
        sort_order = self._extract_sort_order(power_m_query)
        group_by = self._extract_group_by(power_m_query, warnings)
        joins = self._extract_joins(power_m_query, warnings)
        distinct = self._check_distinct(power_m_query)
        limit = self._extract_limit(power_m_query)
        offset = self._extract_offset(power_m_query)

        # Detect unsupported patterns
        self._detect_unsupported(power_m_query, warnings)

        # Build SQL
        sql = self._build_sql(
            source_info=source_info,
            table_name=table_name,
            selected_columns=selected_columns,
            renamed_columns=renamed_columns,
            added_columns=added_columns,
            filters=filters,
            date_filter=date_filter,
            sort_order=sort_order,
            group_by=group_by,
            joins=joins,
            distinct=distinct,
            limit=limit,
            offset=offset,
        )

        return {
            "converted_sql": sql,
            "warnings": warnings,
            "conversion_log": self.conversion_log,
        }

    # ------------------------------------------------------------------ #
    #  let / in parsing                                                   #
    # ------------------------------------------------------------------ #

    def _parse_let_in(self, m_query: str) -> Dict[str, str]:
        """Parse let/in block into a dict of step_name -> expression."""
        steps: Dict[str, str] = {}
        # Match the let ... in block
        let_match = re.search(r'\blet\b(.*?)\bin\b', m_query, re.DOTALL | re.IGNORECASE)
        if not let_match:
            return steps

        body = let_match.group(1)
        # Split on lines that look like  StepName = ...
        # M uses comma-separated assignments inside let
        for m in re.finditer(r'(\w+)\s*=\s*(.*?)(?:,\s*$|\s*$)', body, re.MULTILINE):
            name = m.group(1).strip()
            expr = m.group(2).strip().rstrip(',')
            if name and expr:
                steps[name] = expr
                self._log(f"Parsed step: {name}")

        return steps

    # ------------------------------------------------------------------ #
    #  Extraction helpers                                                 #
    # ------------------------------------------------------------------ #

    def _extract_source(self, m_query: str) -> Dict:
        """Extract data source information."""
        source_info: Dict[str, str] = {}

        if 'Salesforce.Data' in m_query:
            match = re.search(r'Salesforce\.Data\("([^"]+)"', m_query)
            if match:
                source_info['type'] = 'Salesforce'
                source_info['url'] = match.group(1)
                self._log('Detected Salesforce data source')
        elif 'Sql.Database' in m_query or 'Sql.Databases' in m_query:
            source_info['type'] = 'SQLServer'
            server_match = re.search(r'Sql\.Databases?\("([^"]+)"', m_query)
            if server_match:
                source_info['server'] = server_match.group(1)
            self._log('Detected SQL Server data source')
        elif 'Oracle.Database' in m_query:
            source_info['type'] = 'Oracle'
            self._log('Detected Oracle data source')
        elif 'Odbc.DataSource' in m_query or 'Odbc.Query' in m_query:
            source_info['type'] = 'ODBC'
            self._log('Detected ODBC data source')
        elif 'Excel.Workbook' in m_query:
            source_info['type'] = 'Excel'
            self._log('Detected Excel data source')
        elif 'Csv.Document' in m_query:
            source_info['type'] = 'CSV'
            self._log('Detected CSV data source')
        elif 'SharePoint' in m_query:
            source_info['type'] = 'SharePoint'
            self._log('Detected SharePoint data source')
        else:
            source_info['type'] = 'Unknown'
            self._log('WARNING: Could not detect data source type')

        return source_info

    def _extract_table_name(self, m_query: str) -> str:
        """Extract table name from M query."""
        # [Name="TableName"] pattern
        match = re.search(r'\[Name="([^"]+)"\]', m_query)
        if match:
            table_name = match.group(1)
            self._log(f'Extracted table name: {table_name}')
            return table_name.lower().replace(' ', '_')

        # Source{[Name="TableName"]}[Data]
        match = re.search(r'Source\{\[Name="([^"]+)"\]\}\[Data\]', m_query)
        if match:
            table_name = match.group(1)
            self._log(f'Extracted object name: {table_name}')
            return table_name.lower().replace(' ', '_')

        # Sql.Database("server", "db", [Query="SELECT ... FROM table"])
        match = re.search(r'\[Query="([^"]+)"\]', m_query)
        if match:
            # Try to extract FROM clause table
            from_match = re.search(r'\bFROM\s+(\S+)', match.group(1), re.IGNORECASE)
            if from_match:
                table_name = from_match.group(1).strip('[]').replace('.', '_')
                self._log(f'Extracted table from embedded query: {table_name}')
                return table_name.lower()

        return 'unknown_table'

    def _extract_selected_columns(self, m_query: str) -> List[str]:
        """Extract columns from Table.SelectColumns."""
        match = re.search(r'Table\.SelectColumns\([^,]+,\s*\{([^}]+)\}', m_query)
        if match:
            columns = re.findall(r'"([^"]+)"', match.group(1))
            self._log(f'Found {len(columns)} selected columns')
            return columns

        self._log('No column selection found, using SELECT *')
        return ['*']

    def _extract_renamed_columns(self, m_query: str) -> Dict[str, str]:
        """Extract column renames from Table.RenameColumns."""
        renames: Dict[str, str] = {}
        match = re.search(r'Table\.RenameColumns\([^,]+,\s*\{(.*?)\}\s*\)', m_query, re.DOTALL)
        if match:
            pairs = re.findall(r'\{\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\}', match.group(1))
            for old_name, new_name in pairs:
                renames[old_name] = new_name
                self._log(f'Column rename: {old_name} -> {new_name}')
        return renames

    def _extract_added_columns(self, m_query: str, warnings: List[str]) -> List[Dict]:
        """Extract computed columns from Table.AddColumn."""
        added: List[Dict] = []
        for match in re.finditer(
            r'Table\.AddColumn\([^,]+,\s*"([^"]+)"\s*,\s*each\s+(.*?)(?:,\s*type\s+\w+)?\s*\)',
            m_query,
            re.DOTALL,
        ):
            col_name = match.group(1)
            expr_raw = match.group(2).strip()
            sql_expr = self._m_expr_to_sql(expr_raw, warnings)
            added.append({"name": col_name, "expression": sql_expr})
            self._log(f'Added column: {col_name} = {sql_expr}')
        return added

    def _extract_filters(self, m_query: str) -> List[str]:
        """Extract row filters from Table.SelectRows."""
        filters: List[str] = []
        for match in re.finditer(
            r'Table\.SelectRows\([^,]+,\s*each\s+(.*?)\)',
            m_query,
        ):
            raw = match.group(1).strip()
            sql_cond = self._m_filter_to_sql(raw)
            if sql_cond:
                filters.append(sql_cond)
                self._log(f'Extracted filter: {sql_cond}')
        return filters

    def _extract_date_filter(self, m_query: str) -> Optional[str]:
        """Extract date filtering from Date.IsInPrevious* functions."""
        # Date.IsInPreviousNDays
        match = re.search(r'Date\.IsInPreviousNDays\(\[([^\]]+)\],\s*(\d+)\)', m_query)
        if match:
            col, days = match.group(1), match.group(2)
            if days == '365':
                self._log(f'Converted Date.IsInPreviousNDays(365) to 12 months')
                return f'{col} >= CURRENT_DATE() - INTERVAL 12 MONTHS'
            self._log(f'Converted Date.IsInPreviousNDays({days})')
            return f'{col} >= CURRENT_DATE() - INTERVAL {days} DAYS'

        # Date.IsInPreviousNMonths
        match = re.search(r'Date\.IsInPreviousNMonths\(\[([^\]]+)\],\s*(\d+)\)', m_query)
        if match:
            col, months = match.group(1), match.group(2)
            self._log(f'Converted Date.IsInPreviousNMonths({months})')
            return f'{col} >= CURRENT_DATE() - INTERVAL {months} MONTHS'

        # Date.IsInPreviousNYears
        match = re.search(r'Date\.IsInPreviousNYears\(\[([^\]]+)\],\s*(\d+)\)', m_query)
        if match:
            col, years = match.group(1), match.group(2)
            self._log(f'Converted Date.IsInPreviousNYears({years})')
            return f'{col} >= CURRENT_DATE() - INTERVAL {years} YEARS'

        return None

    def _extract_sort_order(self, m_query: str) -> Optional[str]:
        """Extract ORDER BY from Table.Sort."""
        match = re.search(
            r'Table\.Sort\([^,]+,\s*\{\{"([^"]+)"\s*,\s*Order\.(\w+)\}\}',
            m_query,
        )
        if match:
            col = match.group(1)
            direction = 'DESC' if match.group(2) == 'Descending' else 'ASC'
            self._log(f'Sort: {col} {direction}')
            return f'{col} {direction}'

        # Multiple sort columns: {{"col1", Order.Ascending}, {"col2", Order.Descending}}
        multi = re.findall(
            r'\{"([^"]+)"\s*,\s*Order\.(\w+)\}',
            m_query,
        )
        if multi:
            parts = []
            for col, direction in multi:
                d = 'DESC' if direction == 'Descending' else 'ASC'
                parts.append(f'{col} {d}')
            return ', '.join(parts)

        return None

    def _extract_group_by(self, m_query: str, warnings: List[str]) -> Optional[Dict]:
        """Extract GROUP BY from Table.Group."""
        match = re.search(
            r'Table\.Group\([^,]+,\s*\{([^}]+)\}\s*,\s*\{(.*?)\}\s*\)',
            m_query,
            re.DOTALL,
        )
        if not match:
            return None

        group_cols_raw = match.group(1)
        agg_raw = match.group(2)

        group_cols = re.findall(r'"([^"]+)"', group_cols_raw)
        self._log(f'GROUP BY columns: {group_cols}')

        # Parse aggregations: {"AggName", each List.Sum([Col]), type number}
        aggs: List[Dict] = []
        for agg_match in re.finditer(
            r'\{\s*"([^"]+)"\s*,\s*each\s+(.*?)(?:,\s*type\s+\w+)?\s*\}',
            agg_raw,
        ):
            agg_name = agg_match.group(1)
            agg_expr = agg_match.group(2).strip()
            sql_agg = self._m_agg_to_sql(agg_expr, warnings)
            aggs.append({"name": agg_name, "expression": sql_agg})
            self._log(f'Aggregation: {agg_name} = {sql_agg}')

        return {"columns": group_cols, "aggregations": aggs}

    def _extract_joins(self, m_query: str, warnings: List[str]) -> List[Dict]:
        """Extract JOIN from Table.Join or Table.NestedJoin."""
        joins: List[Dict] = []

        for match in re.finditer(
            r'Table\.(?:Nested)?Join\(\s*(\w+)\s*,\s*\{?"([^"]+)"?\}?\s*,\s*(\w+)\s*,\s*\{?"([^"]+)"?\}?',
            m_query,
        ):
            left_table = match.group(1)
            left_col = match.group(2)
            right_table = match.group(3)
            right_col = match.group(4)
            joins.append({
                "left_table": left_table,
                "left_col": left_col,
                "right_table": right_table,
                "right_col": right_col,
            })
            self._log(f'JOIN: {left_table}.{left_col} = {right_table}.{right_col}')

        # Table.ExpandRecordColumn (post-join expand)
        for match in re.finditer(
            r'Table\.ExpandRecordColumn\([^,]+,\s*"([^"]+)"\s*,\s*\{([^}]+)\}',
            m_query,
        ):
            expand_col = match.group(1)
            expand_fields = re.findall(r'"([^"]+)"', match.group(2))
            warnings.append(
                f"Table.ExpandRecordColumn on '{expand_col}' with fields "
                f"{expand_fields} -- verify join columns in output SQL"
            )
            self._log(f'ExpandRecordColumn: {expand_col} -> {expand_fields}')

        return joins

    def _check_distinct(self, m_query: str) -> bool:
        """Check for Table.Distinct."""
        if 'Table.Distinct' in m_query:
            self._log('DISTINCT detected')
            return True
        return False

    def _extract_limit(self, m_query: str) -> Optional[int]:
        """Extract LIMIT from Table.FirstN."""
        match = re.search(r'Table\.FirstN\([^,]+,\s*(\d+)\)', m_query)
        if match:
            n = int(match.group(1))
            self._log(f'LIMIT {n}')
            return n
        return None

    def _extract_offset(self, m_query: str) -> Optional[int]:
        """Extract OFFSET from Table.Skip."""
        match = re.search(r'Table\.Skip\([^,]+,\s*(\d+)\)', m_query)
        if match:
            n = int(match.group(1))
            self._log(f'OFFSET {n}')
            return n
        return None

    # ------------------------------------------------------------------ #
    #  M expression -> SQL helpers                                        #
    # ------------------------------------------------------------------ #

    def _m_expr_to_sql(self, expr: str, warnings: List[str]) -> str:
        """Convert a simple M expression to SQL."""
        result = expr
        # [ColumnName] -> ColumnName
        result = re.sub(r'\[([^\]]+)\]', r'\1', result)
        # Text.Upper -> UPPER
        result = re.sub(r'Text\.Upper\(([^)]+)\)', r'UPPER(\1)', result)
        # Text.Lower -> LOWER
        result = re.sub(r'Text\.Lower\(([^)]+)\)', r'LOWER(\1)', result)
        # Text.Trim -> TRIM
        result = re.sub(r'Text\.Trim\(([^)]+)\)', r'TRIM(\1)', result)
        # Text.Length -> LENGTH
        result = re.sub(r'Text\.Length\(([^)]+)\)', r'LENGTH(\1)', result)
        # Number.Round -> ROUND
        result = re.sub(r'Number\.Round\(([^)]+)\)', r'ROUND(\1)', result)
        # & (string concat) -> CONCAT
        if ' & ' in result:
            parts = [p.strip() for p in result.split('&')]
            result = f"CONCAT({', '.join(parts)})"
        # If we still have M-looking syntax, warn
        if re.search(r'[A-Z]\w+\.\w+\(', result):
            warnings.append(f"M expression may need manual review: {expr}")
        return result

    def _m_filter_to_sql(self, raw: str) -> Optional[str]:
        """Convert M filter expression to SQL WHERE clause."""
        cond = raw
        # [Col] -> Col
        cond = re.sub(r'\[([^\]]+)\]', r'\1', cond)
        # <> -> !=
        cond = cond.replace('<>', '!=')
        # "and" -> AND, "or" -> OR
        cond = re.sub(r'\band\b', 'AND', cond, flags=re.IGNORECASE)
        cond = re.sub(r'\bor\b', 'OR', cond, flags=re.IGNORECASE)
        # null -> IS NULL / IS NOT NULL
        cond = re.sub(r'(\w+)\s*!=\s*null', r'\1 IS NOT NULL', cond, flags=re.IGNORECASE)
        cond = re.sub(r'(\w+)\s*=\s*null', r'\1 IS NULL', cond, flags=re.IGNORECASE)
        return cond.strip() if cond.strip() else None

    def _m_agg_to_sql(self, expr: str, warnings: List[str]) -> str:
        """Convert M aggregation expression to SQL."""
        # List.Sum([Col]) -> SUM(Col)
        match = re.match(r'List\.Sum\(\[([^\]]+)\]\)', expr)
        if match:
            return f'SUM({match.group(1)})'
        # List.Average([Col]) -> AVG(Col)
        match = re.match(r'List\.Average\(\[([^\]]+)\]\)', expr)
        if match:
            return f'AVG({match.group(1)})'
        # List.Max([Col]) -> MAX(Col)
        match = re.match(r'List\.Max\(\[([^\]]+)\]\)', expr)
        if match:
            return f'MAX({match.group(1)})'
        # List.Min([Col]) -> MIN(Col)
        match = re.match(r'List\.Min\(\[([^\]]+)\]\)', expr)
        if match:
            return f'MIN({match.group(1)})'
        # List.Count() or Table.RowCount -> COUNT(*)
        if 'List.Count' in expr or 'Table.RowCount' in expr:
            return 'COUNT(*)'
        # List.Distinct + List.Count -> COUNT(DISTINCT col)
        match = re.match(r'List\.Count\(List\.Distinct\(\[([^\]]+)\]\)\)', expr)
        if match:
            return f'COUNT(DISTINCT {match.group(1)})'

        warnings.append(f"Unsupported aggregation, needs manual review: {expr}")
        return f'/* {expr} */'

    # ------------------------------------------------------------------ #
    #  Unsupported pattern detection                                      #
    # ------------------------------------------------------------------ #

    def _detect_unsupported(self, m_query: str, warnings: List[str]):
        """Warn about M constructs we cannot auto-convert."""
        unsupported = [
            ('Table.Pivot', 'PIVOT -- requires manual conversion'),
            ('Table.Unpivot', 'UNPIVOT -- requires manual conversion'),
            ('Table.TransformColumns', 'Column transforms -- verify output'),
            ('Table.FillDown', 'LAST_VALUE window function -- manual conversion'),
            ('Table.FillUp', 'FIRST_VALUE window function -- manual conversion'),
            ('Table.Buffer', 'No Databricks equivalent (memory hint) -- safely ignored'),
            ('Table.Combine', 'UNION ALL -- verify table schemas match'),
            ('List.Generate', 'Recursive generation -- requires manual conversion'),
        ]
        for pattern, message in unsupported:
            if pattern in m_query:
                warnings.append(f"Unsupported M function: {pattern} -> {message}")
                self._log(f'WARNING: {message}')

    # ------------------------------------------------------------------ #
    #  SQL builder                                                        #
    # ------------------------------------------------------------------ #

    def _build_sql(
        self,
        source_info: Dict,
        table_name: str,
        selected_columns: List[str],
        renamed_columns: Dict[str, str],
        added_columns: List[Dict],
        filters: List[str],
        date_filter: Optional[str],
        sort_order: Optional[str],
        group_by: Optional[Dict],
        joins: List[Dict],
        distinct: bool,
        limit: Optional[int],
        offset: Optional[int],
    ) -> str:
        """Build Databricks SQL from extracted components."""

        # Build FROM table reference
        if self.target_catalog and self.target_schema:
            from_ref = f'{self.target_catalog}.{self.target_schema}.{table_name}'
        else:
            from_ref = table_name

        # Column list
        if group_by:
            col_parts = list(group_by["columns"])
            for agg in group_by["aggregations"]:
                col_parts.append(f'{agg["expression"]} AS {agg["name"]}')
            columns_sql = ',\n  '.join(col_parts)
        else:
            if selected_columns == ['*']:
                col_parts = ['*']
            else:
                col_parts = []
                for c in selected_columns:
                    if c in renamed_columns:
                        col_parts.append(f'{c} AS {renamed_columns[c]}')
                    else:
                        col_parts.append(c)
            # Add computed columns
            for ac in added_columns:
                col_parts.append(f'{ac["expression"]} AS {ac["name"]}')
            columns_sql = ',\n  '.join(col_parts)

        # SELECT clause
        distinct_kw = "DISTINCT " if distinct else ""
        select_clause = f"SELECT\n  {distinct_kw}{columns_sql}"

        # FROM clause
        from_clause = f"FROM {from_ref}"

        # JOIN clauses
        join_clauses = ""
        for j in joins:
            rt = j["right_table"]
            join_clauses += (
                f"\n  JOIN {rt} ON {from_ref}.{j['left_col']} = {rt}.{j['right_col']}"
            )

        # WHERE clause
        all_filters = list(filters)
        if date_filter:
            all_filters.append(date_filter)
        where_clause = f"\nWHERE {' AND '.join(all_filters)}" if all_filters else ""

        # GROUP BY clause
        group_clause = ""
        if group_by:
            group_clause = f"\nGROUP BY {', '.join(group_by['columns'])}"

        # ORDER BY clause
        order_clause = f"\nORDER BY {sort_order}" if sort_order else ""

        # LIMIT / OFFSET
        limit_clause = f"\nLIMIT {limit}" if limit is not None else ""
        offset_clause = f"\nOFFSET {offset}" if offset is not None else ""

        # Assemble
        sql = (
            f"-- Converted from Power Query M\n"
            f"-- Source: {source_info.get('type', 'Unknown')}\n"
            f"-- Target Table: {table_name}\n"
            f"\n"
            f"{select_clause}\n"
            f"{from_clause}{join_clauses}{where_clause}{group_clause}"
            f"{order_clause}{limit_clause}{offset_clause}"
        )

        self._log('Generated Databricks SQL query')
        return sql

    # ------------------------------------------------------------------ #
    #  Logging                                                            #
    # ------------------------------------------------------------------ #

    def _log(self, message: str):
        self.conversion_log.append({'message': message, 'timestamp': None})
        logger.info(message)
