"""Schema comparison endpoints: compare schemas, tables, data type mappings."""
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, HTTPException
from databricks import sql

from app.config import DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
from app.models import (
    SchemaComparisonRequest, SchemaComparisonResponse,
    TableComparisonRequest, ColumnDifference, TableComparison,
    DataTypeMappingResponse,
)
from app.state import active_connections
from app.utils import quote_identifier, validate_identifier
from app.dependencies import get_databricks_connection

router = APIRouter()


def get_source_table_metadata(connection_id: str, catalog: Optional[str], schema: str, table: str) -> Dict[str, Any]:
    """Get table metadata from source system"""
    if connection_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")

    conn_info = active_connections[connection_id]
    source_type = conn_info["source_type"]

    # For now, return simulated data
    # In production, this would query the actual source system
    return {
        "table_name": table,
        "schema": schema,
        "catalog": catalog,
        "columns": [
            {"name": "id", "type": "NUMBER", "nullable": False, "primary_key": True},
            {"name": "name", "type": "VARCHAR2", "nullable": True, "primary_key": False},
            {"name": "created_date", "type": "DATE", "nullable": True, "primary_key": False}
        ],
        "indexes": ["idx_name"],
        "primary_keys": ["id"],
        "foreign_keys": []
    }


def get_target_table_metadata(catalog: str, schema: str, table: str) -> Dict[str, Any]:
    """Get table metadata from Databricks target"""
    try:
        safe_catalog = quote_identifier(catalog)
        safe_schema = quote_identifier(schema)
        safe_table = quote_identifier(table)

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                # Get column information
                cursor.execute(f"DESCRIBE EXTENDED {safe_catalog}.{safe_schema}.{safe_table}")
                describe_result = cursor.fetchall()

                columns = []
                found_partition_info = False
                for row in describe_result:
                    if row[0] and row[0].strip() == '':
                        continue
                    if row[0] and row[0].startswith('#'):
                        found_partition_info = True
                        break
                    if not found_partition_info and row[0] and row[0] not in ['', '# col_name']:
                        columns.append({
                            "name": row[0],
                            "type": row[1] if len(row) > 1 else "unknown",
                            "nullable": True,  # Databricks doesn't enforce NOT NULL
                            "comment": row[2] if len(row) > 2 else None
                        })

                # Try to get primary key info
                primary_keys = []
                try:
                    cursor.execute(f"SHOW TBLPROPERTIES {safe_catalog}.{safe_schema}.{safe_table}")
                    props = cursor.fetchall()
                    for prop in props:
                        if prop[0] and 'primary' in prop[0].lower():
                            # Extract primary key columns if available
                            pass
                except:
                    pass

                return {
                    "table_name": table,
                    "schema": schema,
                    "catalog": catalog,
                    "columns": columns,
                    "indexes": [],
                    "primary_keys": primary_keys,
                    "foreign_keys": []
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get target table metadata: {str(e)}")


@router.post("/compare/schemas", response_model=SchemaComparisonResponse)
async def compare_schemas(request: SchemaComparisonRequest):
    """Compare source and target schemas to identify migration differences"""
    try:
        # Validate connection
        if request.source_connection_id not in active_connections:
            return SchemaComparisonResponse(
                success=False,
                source_info={},
                target_info={},
                summary={},
                error="Source connection not found"
            )

        conn_info = active_connections[request.source_connection_id]
        source_type = conn_info["source_type"]

        # Get target schema tables
        safe_target_catalog = quote_identifier(request.target_catalog)
        safe_target_schema = quote_identifier(request.target_schema)

        target_tables = []
        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                try:
                    cursor.execute(f"SHOW TABLES IN {safe_target_catalog}.{safe_target_schema}")
                    target_tables = [row[1] for row in cursor.fetchall()]
                except:
                    target_tables = []

        # For demonstration, simulate source tables
        # In production, this would query the actual source system
        source_tables = ["customers", "orders", "products", "order_items"]

        # Compare tables
        tables_only_in_source = list(set(source_tables) - set(target_tables))
        tables_only_in_target = list(set(target_tables) - set(source_tables))
        common_tables = list(set(source_tables) & set(target_tables))

        tables_in_both = []
        for table in common_tables:
            # For common tables, we could do detailed column comparison
            # For now, mark them as matching
            tables_in_both.append(TableComparison(
                table_name=table,
                status="match",
                column_differences=[],
                source_column_count=0,
                target_column_count=0
            ))

        summary = {
            "total_source_tables": len(source_tables),
            "total_target_tables": len(target_tables),
            "tables_to_create": len(tables_only_in_source),
            "orphaned_tables": len(tables_only_in_target),
            "matching_tables": len([t for t in tables_in_both if t.status == "match"]),
            "tables_with_differences": len([t for t in tables_in_both if t.status == "different"])
        }

        return SchemaComparisonResponse(
            success=True,
            source_info={
                "type": source_type,
                "host": conn_info["host"],
                "database": conn_info["database"],
                "schema": request.source_schema or "public"
            },
            target_info={
                "catalog": request.target_catalog,
                "schema": request.target_schema
            },
            tables_only_in_source=tables_only_in_source,
            tables_only_in_target=tables_only_in_target,
            tables_in_both=tables_in_both,
            summary=summary
        )

    except Exception as e:
        return SchemaComparisonResponse(
            success=False,
            source_info={},
            target_info={},
            summary={},
            error=str(e)
        )


@router.post("/compare/tables/{table_name}")
async def compare_table_structures(table_name: str, request: TableComparisonRequest):
    """Compare specific table structures between source and target"""
    try:
        # Validate identifiers
        safe_table = validate_identifier(table_name)

        # Get source table metadata
        source_meta = get_source_table_metadata(
            request.source_connection_id,
            request.source_catalog,
            request.source_schema,
            request.source_table
        )

        # Get target table metadata
        try:
            target_meta = get_target_table_metadata(
                request.target_catalog,
                request.target_schema,
                request.target_table
            )
        except HTTPException as e:
            if e.status_code == 500:
                # Table doesn't exist in target
                return {
                    "success": True,
                    "table_name": table_name,
                    "exists_in_target": False,
                    "source_columns": source_meta["columns"],
                    "target_columns": [],
                    "differences": [{"type": "missing_table", "message": "Table does not exist in target"}]
                }
            raise

        # Compare columns
        source_cols = {col["name"]: col for col in source_meta["columns"]}
        target_cols = {col["name"]: col for col in target_meta["columns"]}

        column_differences = []

        # Find columns only in source
        for col_name, col_info in source_cols.items():
            if col_name not in target_cols:
                column_differences.append(ColumnDifference(
                    column_name=col_name,
                    difference_type="missing_in_target",
                    source_type=col_info["type"],
                    source_nullable=col_info.get("nullable")
                ))

        # Find columns only in target
        for col_name, col_info in target_cols.items():
            if col_name not in source_cols:
                column_differences.append(ColumnDifference(
                    column_name=col_name,
                    difference_type="missing_in_source",
                    target_type=col_info["type"],
                    target_nullable=col_info.get("nullable")
                ))

        # Find columns with differences
        for col_name in set(source_cols.keys()) & set(target_cols.keys()):
            source_col = source_cols[col_name]
            target_col = target_cols[col_name]

            # Compare data types (simplified comparison)
            if source_col["type"].upper() != target_col["type"].upper():
                column_differences.append(ColumnDifference(
                    column_name=col_name,
                    difference_type="type_mismatch",
                    source_type=source_col["type"],
                    target_type=target_col["type"],
                    source_nullable=source_col.get("nullable"),
                    target_nullable=target_col.get("nullable")
                ))
            elif source_col.get("nullable") != target_col.get("nullable"):
                column_differences.append(ColumnDifference(
                    column_name=col_name,
                    difference_type="nullability_mismatch",
                    source_type=source_col["type"],
                    target_type=target_col["type"],
                    source_nullable=source_col.get("nullable"),
                    target_nullable=target_col.get("nullable")
                ))

        return {
            "success": True,
            "table_name": table_name,
            "exists_in_target": True,
            "source_columns": source_meta["columns"],
            "target_columns": target_meta["columns"],
            "column_differences": [diff.dict() for diff in column_differences],
            "index_differences": {
                "source_indexes": source_meta.get("indexes", []),
                "target_indexes": target_meta.get("indexes", [])
            },
            "primary_key_differences": {
                "source_primary_keys": source_meta.get("primary_keys", []),
                "target_primary_keys": target_meta.get("primary_keys", [])
            },
            "foreign_key_differences": {
                "source_foreign_keys": source_meta.get("foreign_keys", []),
                "target_foreign_keys": target_meta.get("foreign_keys", [])
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compare tables: {str(e)}")


@router.get("/compare/data-types")
async def get_data_type_mappings(source_system: str):
    """Get data type mapping information for a source system"""

    # Data type mappings for different source systems
    data_type_mappings = {
        "oracle": [
            {"source": "NUMBER", "target": "DECIMAL", "notes": "Precision preserved"},
            {"source": "NUMBER(p,s)", "target": "DECIMAL(p,s)", "notes": "Exact mapping"},
            {"source": "VARCHAR2", "target": "STRING", "notes": "No length limit in Databricks"},
            {"source": "CHAR", "target": "STRING", "notes": "No padding in Databricks"},
            {"source": "DATE", "target": "DATE", "notes": "Direct mapping"},
            {"source": "TIMESTAMP", "target": "TIMESTAMP", "notes": "Direct mapping"},
            {"source": "CLOB", "target": "STRING", "notes": "Large text"},
            {"source": "BLOB", "target": "BINARY", "notes": "Binary data"},
            {"source": "RAW", "target": "BINARY", "notes": "Binary data"},
            {"source": "LONG", "target": "STRING", "notes": "Deprecated in Oracle"}
        ],
        "snowflake": [
            {"source": "NUMBER", "target": "DECIMAL", "notes": "Precision preserved"},
            {"source": "VARCHAR", "target": "STRING", "notes": "Direct mapping"},
            {"source": "TEXT", "target": "STRING", "notes": "Direct mapping"},
            {"source": "DATE", "target": "DATE", "notes": "Direct mapping"},
            {"source": "TIMESTAMP_NTZ", "target": "TIMESTAMP", "notes": "No timezone"},
            {"source": "TIMESTAMP_TZ", "target": "TIMESTAMP", "notes": "Timezone converted"},
            {"source": "BOOLEAN", "target": "BOOLEAN", "notes": "Direct mapping"},
            {"source": "VARIANT", "target": "STRING", "notes": "JSON as string"},
            {"source": "ARRAY", "target": "ARRAY", "notes": "Direct mapping"},
            {"source": "OBJECT", "target": "STRUCT", "notes": "Structure mapping"}
        ],
        "sqlserver": [
            {"source": "INT", "target": "INT", "notes": "Direct mapping"},
            {"source": "BIGINT", "target": "BIGINT", "notes": "Direct mapping"},
            {"source": "VARCHAR", "target": "STRING", "notes": "No length limit"},
            {"source": "NVARCHAR", "target": "STRING", "notes": "Unicode support"},
            {"source": "DATETIME", "target": "TIMESTAMP", "notes": "Direct mapping"},
            {"source": "DATETIME2", "target": "TIMESTAMP", "notes": "Higher precision"},
            {"source": "DATE", "target": "DATE", "notes": "Direct mapping"},
            {"source": "BIT", "target": "BOOLEAN", "notes": "Direct mapping"},
            {"source": "DECIMAL", "target": "DECIMAL", "notes": "Precision preserved"},
            {"source": "MONEY", "target": "DECIMAL(19,4)", "notes": "Fixed precision"}
        ],
        "teradata": [
            {"source": "INTEGER", "target": "INT", "notes": "Direct mapping"},
            {"source": "BIGINT", "target": "BIGINT", "notes": "Direct mapping"},
            {"source": "DECIMAL", "target": "DECIMAL", "notes": "Precision preserved"},
            {"source": "VARCHAR", "target": "STRING", "notes": "No length limit"},
            {"source": "CHAR", "target": "STRING", "notes": "No padding"},
            {"source": "DATE", "target": "DATE", "notes": "Direct mapping"},
            {"source": "TIMESTAMP", "target": "TIMESTAMP", "notes": "Direct mapping"},
            {"source": "CLOB", "target": "STRING", "notes": "Large text"},
            {"source": "BLOB", "target": "BINARY", "notes": "Binary data"}
        ],
        "mysql": [
            {"source": "INT", "target": "INT", "notes": "Direct mapping"},
            {"source": "BIGINT", "target": "BIGINT", "notes": "Direct mapping"},
            {"source": "VARCHAR", "target": "STRING", "notes": "No length limit"},
            {"source": "TEXT", "target": "STRING", "notes": "Large text"},
            {"source": "DATE", "target": "DATE", "notes": "Direct mapping"},
            {"source": "DATETIME", "target": "TIMESTAMP", "notes": "Direct mapping"},
            {"source": "TIMESTAMP", "target": "TIMESTAMP", "notes": "Direct mapping"},
            {"source": "DECIMAL", "target": "DECIMAL", "notes": "Precision preserved"},
            {"source": "BOOLEAN", "target": "BOOLEAN", "notes": "Direct mapping"},
            {"source": "JSON", "target": "STRING", "notes": "JSON as string"}
        ]
    }

    mappings = data_type_mappings.get(source_system.lower(), [])

    return DataTypeMappingResponse(
        source_system=source_system,
        mappings=mappings
    )
