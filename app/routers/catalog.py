"""Unity Catalog browsing endpoints: catalogs, schemas, tables, columns."""
from fastapi import APIRouter, HTTPException
from databricks import sql

from app.config import DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
from app.models import CatalogSchemaResponse
from app.utils import quote_identifier
from app.dependencies import get_databricks_connection

router = APIRouter()


@router.get("/catalogs-schemas", response_model=CatalogSchemaResponse)
async def get_catalogs_schemas():
    """Get list of Unity Catalog catalogs and schemas"""
    try:
        if not DATABRICKS_TOKEN or not DATABRICKS_HTTP_PATH:
            return CatalogSchemaResponse(
                catalogs=["main"],
                schemas={"main": ["default"]}
            )

        catalogs = []
        schemas_dict = {}

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW CATALOGS")
                catalogs = [row[0] for row in cursor.fetchall()]

                for catalog in catalogs:
                    try:
                        # Validate and quote catalog name to prevent SQL injection
                        safe_catalog = quote_identifier(catalog)
                        cursor.execute(f"SHOW SCHEMAS IN {safe_catalog}")
                        schemas_dict[catalog] = [row[0] for row in cursor.fetchall()]
                    except:
                        schemas_dict[catalog] = ["default"]

        return CatalogSchemaResponse(
            catalogs=catalogs if catalogs else ["main"],
            schemas=schemas_dict if schemas_dict else {"main": ["default"]}
        )

    except Exception as e:
        return CatalogSchemaResponse(
            catalogs=["main"],
            schemas={"main": ["default"]}
        )


@router.get("/catalogs")
async def list_catalogs():
    """List available catalogs"""
    try:
        if not DATABRICKS_HOST or not DATABRICKS_TOKEN or not DATABRICKS_HTTP_PATH:
            raise HTTPException(
                status_code=503,
                detail="Databricks credentials not configured"
            )

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW CATALOGS")
                catalogs = [row[0] for row in cursor.fetchall()]
                return {"catalogs": catalogs}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list catalogs: {str(e)}")


@router.get("/catalogs/{catalog_name}/schemas")
async def list_schemas(catalog_name: str):
    """List schemas in a catalog"""
    try:
        # Validate catalog name to prevent SQL injection
        safe_catalog = quote_identifier(catalog_name)

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SHOW SCHEMAS IN {safe_catalog}")
                schemas = [row[0] for row in cursor.fetchall()]
                return {"schemas": schemas}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {str(e)}")


@router.get("/catalogs/{catalog_name}/schemas/{schema_name}/tables")
async def list_tables(catalog_name: str, schema_name: str):
    """List tables in a schema"""
    try:
        # Validate catalog and schema names to prevent SQL injection
        safe_catalog = quote_identifier(catalog_name)
        safe_schema = quote_identifier(schema_name)

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SHOW TABLES IN {safe_catalog}.{safe_schema}")
                tables = [row[1] for row in cursor.fetchall()]  # row[1] is table name
                return {"tables": tables}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {str(e)}")


@router.get("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/columns")
async def list_columns(catalog_name: str, schema_name: str, table_name: str):
    """List columns in a table"""
    try:
        # Validate all identifiers to prevent SQL injection
        safe_catalog = quote_identifier(catalog_name)
        safe_schema = quote_identifier(schema_name)
        safe_table = quote_identifier(table_name)

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"DESCRIBE {safe_catalog}.{safe_schema}.{safe_table}")
                columns = [{"name": row[0], "type": row[1], "comment": row[2] if len(row) > 2 else None}
                          for row in cursor.fetchall()]
                return {"columns": columns}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list columns: {str(e)}")
