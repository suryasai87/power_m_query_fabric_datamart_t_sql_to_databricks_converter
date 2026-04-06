"""Tests for SQL converters."""

import sys
import os
import pytest

# Ensure the project root is on sys.path so imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from converters.tsql.converter import TSQLConverter
from converters.fabric.converter import FabricConverter
from converters.power_m.converter import PowerMConverter
from utils.type_mappings import map_sql_server_type, TYPES_WITH_WARNINGS


# ===================================================================== #
#  T-SQL Converter Tests                                                 #
# ===================================================================== #

class TestTSQLConverter:
    def setup_method(self):
        self.converter = TSQLConverter(target_catalog="test_catalog", target_schema="test_schema")

    def test_simple_select(self):
        result = self.converter.convert("SELECT [Name], [Age] FROM [dbo].[Users]")
        sql = result["converted_sql"]
        assert "test_catalog.test_schema" in sql
        assert result["method"] in ("sqlglot_ast", "regex_fallback")

    def test_getdate_conversion(self):
        result = self.converter.convert("SELECT GETDATE()")
        sql = result["converted_sql"].upper()
        assert "CURRENT_TIMESTAMP" in sql

    def test_isnull_to_coalesce(self):
        result = self.converter.convert("SELECT ISNULL(col1, 'default') FROM t")
        sql = result["converted_sql"].upper()
        assert "COALESCE" in sql

    def test_top_n(self):
        result = self.converter.convert("SELECT TOP 10 * FROM users")
        sql = result["converted_sql"].upper()
        # sqlglot should convert TOP 10 to LIMIT 10
        assert "LIMIT" in sql or "TOP" not in sql

    def test_nolock_removed(self):
        result = self.converter.convert(
            "SELECT * FROM Orders WITH (NOLOCK) WHERE id = 1"
        )
        sql = result["converted_sql"]
        assert "NOLOCK" not in sql

    def test_set_nocount_removed(self):
        result = self.converter.convert(
            "SET NOCOUNT ON; SELECT 1"
        )
        sql = result["converted_sql"]
        assert "NOCOUNT" not in sql

    def test_ddl_uses_delta(self):
        ddl = "CREATE TABLE [dbo].[Users] ([Id] INT, [Name] NVARCHAR(100))"
        result = self.converter.convert_ddl(ddl)
        sql = result["converted_sql"].upper()
        assert "USING DELTA" in sql

    def test_ddl_type_conversion(self):
        ddl = "CREATE TABLE t (id INT, name NVARCHAR(100), active BIT)"
        result = self.converter.convert_ddl(ddl)
        sql = result["converted_sql"].upper()
        # sqlglot should convert BIT to BOOLEAN and NVARCHAR to VARCHAR/STRING
        assert "BOOLEAN" in sql or "BIT" not in sql

    def test_dbo_rewrite(self):
        result = self.converter.convert("SELECT * FROM dbo.Orders")
        sql = result["converted_sql"]
        assert "test_catalog.test_schema.Orders" in sql

    def test_convert_query_legacy_api(self):
        """Test backward-compatible convert_query returns tuple."""
        sql, notes = self.converter.convert_query("SELECT GETDATE()")
        assert isinstance(sql, str)
        assert isinstance(notes, list)
        assert "CURRENT_TIMESTAMP" in sql.upper()

    def test_multiple_statements(self):
        sql_text = "SELECT 1; SELECT 2"
        result = self.converter.convert(sql_text)
        sql = result["converted_sql"]
        # Should contain both statements
        assert "1" in sql
        assert "2" in sql

    def test_len_to_length(self):
        """Verify LEN -> LENGTH in regex fallback path."""
        conv = TSQLConverter(target_catalog="c", target_schema="s")
        converted = conv._regex_fallback("SELECT LEN(name) FROM t")
        assert "LENGTH" in converted


# ===================================================================== #
#  Fabric Converter Tests                                                #
# ===================================================================== #

class TestFabricConverter:
    def setup_method(self):
        self.converter = FabricConverter(target_catalog="fab_cat", target_schema="fab_schema")

    def test_external_table_removed(self):
        result = self.converter.convert("CREATE EXTERNAL TABLE t (id INT)")
        sql = result["converted_sql"]
        assert "EXTERNAL" not in sql

    def test_source_type_tag(self):
        result = self.converter.convert("SELECT 1")
        assert result.get("source_type") == "fabric_datamart"

    def test_distribution_removed(self):
        ddl = (
            "CREATE TABLE t (id INT) "
            "WITH (DISTRIBUTION = HASH(id), CLUSTERED COLUMNSTORE INDEX)"
        )
        result = self.converter.convert(ddl)
        sql = result["converted_sql"]
        assert "DISTRIBUTION" not in sql
        assert "CLUSTERED COLUMNSTORE" not in sql

    def test_option_label_removed(self):
        result = self.converter.convert(
            "SELECT * FROM t OPTION (LABEL = 'my_query')"
        )
        sql = result["converted_sql"]
        assert "OPTION" not in sql
        assert "LABEL" not in sql

    def test_inherits_tsql_conversion(self):
        """Fabric converter should still handle standard T-SQL."""
        result = self.converter.convert("SELECT GETDATE()")
        sql = result["converted_sql"].upper()
        assert "CURRENT_TIMESTAMP" in sql


# ===================================================================== #
#  Power M Converter Tests                                               #
# ===================================================================== #

class TestPowerMConverter:
    def setup_method(self):
        self.converter = PowerMConverter(target_catalog="my_cat", target_schema="my_schema")

    def test_basic_salesforce_conversion(self):
        m_query = '''let
    Source = Salesforce.Data("https://login.salesforce.com"),
    Navigation = Source{[Name="Account"]}[Data],
    Selected = Table.SelectColumns(Navigation, {"Name", "Industry"})
in
    Selected'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "SELECT" in sql
        assert "Name" in sql
        assert "Industry" in sql
        assert "my_cat.my_schema.account" in sql

    def test_date_filter(self):
        m_query = '''let
    Source = Salesforce.Data("https://login.salesforce.com"),
    Nav = Source{[Name="Opportunity"]}[Data],
    Filtered = Table.SelectRows(Nav, each Date.IsInPreviousNDays([CloseDate], 30))
in
    Filtered'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "INTERVAL 30 DAYS" in sql

    def test_sort_order(self):
        m_query = '''let
    Source = Salesforce.Data("https://x.com"),
    Nav = Source{[Name="Contact"]}[Data],
    Sorted = Table.Sort(Nav, {{"LastName", Order.Ascending}})
in
    Sorted'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "ORDER BY" in sql
        assert "LastName ASC" in sql

    def test_distinct(self):
        m_query = '''let
    Source = Salesforce.Data("https://x.com"),
    Nav = Source{[Name="Lead"]}[Data],
    Unique = Table.Distinct(Nav)
in
    Unique'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "DISTINCT" in sql

    def test_firstn_limit(self):
        m_query = '''let
    Source = Salesforce.Data("https://x.com"),
    Nav = Source{[Name="Lead"]}[Data],
    Top10 = Table.FirstN(Nav, 10)
in
    Top10'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "LIMIT 10" in sql

    def test_skip_offset(self):
        m_query = '''let
    Source = Salesforce.Data("https://x.com"),
    Nav = Source{[Name="Lead"]}[Data],
    Skipped = Table.Skip(Nav, 5)
in
    Skipped'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "OFFSET 5" in sql

    def test_rename_columns(self):
        m_query = '''let
    Source = Salesforce.Data("https://x.com"),
    Nav = Source{[Name="Account"]}[Data],
    Selected = Table.SelectColumns(Nav, {"Name", "Phone"}),
    Renamed = Table.RenameColumns(Selected, {{"Name", "AccountName"}, {"Phone", "PhoneNumber"}})
in
    Renamed'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "AccountName" in sql
        assert "PhoneNumber" in sql

    def test_unsupported_warns(self):
        m_query = '''let
    Source = Salesforce.Data("https://x.com"),
    Nav = Source{[Name="Account"]}[Data],
    Pivoted = Table.Pivot(Nav, "Col", "Val")
in
    Pivoted'''
        result = self.converter.convert(m_query)
        assert any("Table.Pivot" in w for w in result["warnings"])

    def test_sql_server_source(self):
        m_query = '''let
    Source = Sql.Database("myserver.database.windows.net", "mydb"),
    Nav = Source{[Name="Orders"]}[Data]
in
    Nav'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "orders" in sql.lower()

    def test_group_by(self):
        m_query = '''let
    Source = Salesforce.Data("https://x.com"),
    Nav = Source{[Name="Opportunity"]}[Data],
    Grouped = Table.Group(Nav, {"StageName"}, {{"Total", each List.Sum([Amount]), type number}})
in
    Grouped'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "GROUP BY" in sql
        assert "SUM(Amount)" in sql

    def test_date_previous_months(self):
        m_query = '''let
    Source = Salesforce.Data("https://x.com"),
    Nav = Source{[Name="Task"]}[Data],
    Filtered = Table.SelectRows(Nav, each Date.IsInPreviousNMonths([ActivityDate], 6))
in
    Filtered'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "INTERVAL 6 MONTHS" in sql

    def test_365_days_converts_to_months(self):
        m_query = '''let
    Source = Salesforce.Data("https://x.com"),
    Nav = Source{[Name="Case"]}[Data],
    Filtered = Table.SelectRows(Nav, each Date.IsInPreviousNDays([CreatedDate], 365))
in
    Filtered'''
        result = self.converter.convert(m_query)
        sql = result["converted_sql"]
        assert "12 MONTHS" in sql


# ===================================================================== #
#  Type Mapping Tests                                                    #
# ===================================================================== #

class TestTypeMappings:
    def test_int(self):
        assert map_sql_server_type('INT') == 'INT'

    def test_varchar_drops_length(self):
        assert map_sql_server_type('VARCHAR(100)') == 'STRING'

    def test_nvarchar_max(self):
        assert map_sql_server_type('NVARCHAR(MAX)') == 'STRING'

    def test_decimal_preserves_precision(self):
        assert map_sql_server_type('DECIMAL(10,2)') == 'DECIMAL(10,2)'

    def test_bit_to_boolean(self):
        assert map_sql_server_type('BIT') == 'BOOLEAN'

    def test_money(self):
        assert map_sql_server_type('MONEY') == 'DECIMAL(19,4)'

    def test_geography(self):
        assert map_sql_server_type('GEOGRAPHY') == 'STRING'

    def test_geometry(self):
        assert map_sql_server_type('GEOMETRY') == 'STRING'

    def test_hierarchyid(self):
        assert map_sql_server_type('HIERARCHYID') == 'STRING'

    def test_sql_variant(self):
        assert map_sql_server_type('SQL_VARIANT') == 'STRING'

    def test_rowversion(self):
        assert map_sql_server_type('ROWVERSION') == 'BINARY'

    def test_unknown_defaults_to_string(self):
        assert map_sql_server_type('FOOBAR') == 'STRING'

    def test_float_to_double(self):
        assert map_sql_server_type('FLOAT') == 'DOUBLE'

    def test_datetime2_to_timestamp(self):
        assert map_sql_server_type('DATETIME2') == 'TIMESTAMP'

    def test_types_with_warnings_exist(self):
        """Verify warning types are documented."""
        assert 'GEOGRAPHY' in TYPES_WITH_WARNINGS
        assert 'GEOMETRY' in TYPES_WITH_WARNINGS
        assert 'HIERARCHYID' in TYPES_WITH_WARNINGS
