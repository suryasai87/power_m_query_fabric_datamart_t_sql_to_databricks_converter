"""Rollback endpoints: snapshot CRUD, diff, validate, restore, delete."""
import json
import uuid
from typing import List
from datetime import datetime
from fastapi import APIRouter, HTTPException

from app.config import MIGRATION_VOLUME, SNAPSHOT_DIRECTORY
from app.models import (
    CreateSnapshotRequest, CreateSnapshotResponse,
    SnapshotObjectInfo, SnapshotInfo, ListSnapshotsResponse,
    SnapshotDiffResponse, DiffObjectChange,
    RollbackValidationRequest, RollbackValidationResponse, ValidationIssue,
    RestoreSnapshotRequest, RestoreSnapshotResponse, RestoreResult,
    DeleteSnapshotResponse,
)
from app.state import snapshots
from app.utils import quote_identifier
from app.dependencies import get_databricks_connection

router = APIRouter()


@router.post("/rollback/snapshot", response_model=CreateSnapshotResponse)
async def create_snapshot(request: CreateSnapshotRequest):
    """Create a snapshot of database objects before migration"""
    try:
        # Validate catalog and schema names
        safe_catalog = quote_identifier(request.catalog)
        safe_schema = quote_identifier(request.schema_name)

        snapshot_id = str(uuid.uuid4())
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        snapshot_objects: List[SnapshotObjectInfo] = []

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                # Get list of tables in schema
                cursor.execute(f"SHOW TABLES IN {safe_catalog}.{safe_schema}")
                all_tables = [row[1] for row in cursor.fetchall()]

                # Filter tables if specific list provided
                tables_to_snapshot = request.tables if request.tables else all_tables

                for table_name in tables_to_snapshot:
                    if table_name not in all_tables:
                        continue

                    try:
                        safe_table = quote_identifier(table_name)
                        full_table_name = f"{safe_catalog}.{safe_schema}.{safe_table}"

                        # Get table DDL
                        cursor.execute(f"SHOW CREATE TABLE {full_table_name}")
                        ddl_result = cursor.fetchone()
                        ddl = ddl_result[0] if ddl_result else ""

                        # Get Delta Lake version if available
                        version = None
                        if request.include_data:
                            try:
                                cursor.execute(f"DESCRIBE HISTORY {full_table_name} LIMIT 1")
                                history = cursor.fetchone()
                                if history:
                                    version = int(history[0])  # version is first column
                            except:
                                pass

                        # Determine object type
                        object_type = "VIEW" if "CREATE VIEW" in ddl.upper() else "TABLE"

                        snapshot_objects.append(SnapshotObjectInfo(
                            catalog=request.catalog,
                            schema_name=request.schema_name,
                            table_name=table_name,
                            object_type=object_type,
                            ddl=ddl,
                            version=version
                        ))
                    except Exception as table_error:
                        # Continue with other tables if one fails
                        continue

                # Store snapshot metadata
                snapshot_path = f"/Volumes/{MIGRATION_VOLUME}/{SNAPSHOT_DIRECTORY}/{snapshot_id}"
                snapshot_data = {
                    "snapshot_id": snapshot_id,
                    "catalog": request.catalog,
                    "schema_name": request.schema_name,
                    "description": request.description,
                    "created_at": datetime.now().isoformat(),
                    "num_objects": len(snapshot_objects),
                    "tables": [obj.table_name for obj in snapshot_objects],
                    "include_data": request.include_data,
                    "auto_snapshot": request.auto_snapshot,
                    "snapshot_path": snapshot_path,
                    "objects": [obj.dict() for obj in snapshot_objects]
                }

                # Store in memory (in production, save to Delta Lake or Volume)
                snapshots[snapshot_id] = snapshot_data

                # Try to save to volume (optional, may fail if volume not writable)
                try:
                    snapshot_json = json.dumps(snapshot_data, indent=2)
                    # In production, write to volume using Delta Lake or file system
                    pass
                except:
                    pass

                return CreateSnapshotResponse(
                    success=True,
                    snapshot_id=snapshot_id,
                    created_at=snapshot_data["created_at"],
                    num_objects=len(snapshot_objects),
                    snapshot_path=snapshot_path
                )

    except HTTPException:
        raise
    except Exception as e:
        return CreateSnapshotResponse(
            success=False,
            error=f"Failed to create snapshot: {str(e)}"
        )


@router.get("/rollback/snapshots", response_model=ListSnapshotsResponse)
async def list_snapshots():
    """List all available snapshots"""
    try:
        snapshot_list = []

        for snapshot_id, snapshot_data in snapshots.items():
            snapshot_list.append(SnapshotInfo(
                snapshot_id=snapshot_id,
                catalog=snapshot_data["catalog"],
                schema_name=snapshot_data["schema_name"],
                description=snapshot_data["description"],
                created_at=snapshot_data["created_at"],
                num_objects=snapshot_data["num_objects"],
                tables=snapshot_data["tables"],
                include_data=snapshot_data.get("include_data", False),
                auto_snapshot=snapshot_data.get("auto_snapshot", False),
                snapshot_path=snapshot_data["snapshot_path"],
                created_by=snapshot_data.get("created_by")
            ))

        # Sort by created_at descending (newest first)
        snapshot_list.sort(key=lambda x: x.created_at, reverse=True)

        return ListSnapshotsResponse(
            success=True,
            snapshots=snapshot_list
        )
    except Exception as e:
        return ListSnapshotsResponse(
            success=False,
            snapshots=[],
            error=str(e)
        )


@router.get("/rollback/diff/{snapshot_id}", response_model=SnapshotDiffResponse)
async def get_snapshot_diff(snapshot_id: str):
    """Show differences between snapshot and current state"""
    try:
        if snapshot_id not in snapshots:
            raise HTTPException(status_code=404, detail="Snapshot not found")

        snapshot_data = snapshots[snapshot_id]
        catalog = snapshot_data["catalog"]
        schema_name = snapshot_data["schema_name"]

        safe_catalog = quote_identifier(catalog)
        safe_schema = quote_identifier(schema_name)

        changes: List[DiffObjectChange] = []

        # Get snapshot objects
        snapshot_objects = {obj["table_name"]: obj for obj in snapshot_data["objects"]}

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                # Get current tables
                cursor.execute(f"SHOW TABLES IN {safe_catalog}.{safe_schema}")
                current_tables = {row[1]: True for row in cursor.fetchall()}

                # Check for modified and deleted objects
                for table_name, snapshot_obj in snapshot_objects.items():
                    if table_name not in current_tables:
                        # Object was deleted
                        changes.append(DiffObjectChange(
                            object_name=table_name,
                            object_type=snapshot_obj["object_type"],
                            change_type="DELETED",
                            snapshot_ddl=snapshot_obj["ddl"],
                            current_ddl=None,
                            diff_summary=f"{snapshot_obj['object_type']} was deleted after snapshot"
                        ))
                    else:
                        # Object exists, check if modified
                        try:
                            safe_table = quote_identifier(table_name)
                            full_table_name = f"{safe_catalog}.{safe_schema}.{safe_table}"
                            cursor.execute(f"SHOW CREATE TABLE {full_table_name}")
                            current_ddl = cursor.fetchone()[0]

                            if current_ddl != snapshot_obj["ddl"]:
                                changes.append(DiffObjectChange(
                                    object_name=table_name,
                                    object_type=snapshot_obj["object_type"],
                                    change_type="MODIFIED",
                                    snapshot_ddl=snapshot_obj["ddl"],
                                    current_ddl=current_ddl,
                                    diff_summary=f"{snapshot_obj['object_type']} structure was modified"
                                ))
                            else:
                                changes.append(DiffObjectChange(
                                    object_name=table_name,
                                    object_type=snapshot_obj["object_type"],
                                    change_type="UNCHANGED",
                                    snapshot_ddl=snapshot_obj["ddl"],
                                    current_ddl=current_ddl,
                                    diff_summary="No changes detected"
                                ))
                        except:
                            pass

                # Check for newly created objects
                for table_name in current_tables:
                    if table_name not in snapshot_objects:
                        try:
                            safe_table = quote_identifier(table_name)
                            full_table_name = f"{safe_catalog}.{safe_schema}.{safe_table}"
                            cursor.execute(f"SHOW CREATE TABLE {full_table_name}")
                            current_ddl = cursor.fetchone()[0]
                            object_type = "VIEW" if "CREATE VIEW" in current_ddl.upper() else "TABLE"

                            changes.append(DiffObjectChange(
                                object_name=table_name,
                                object_type=object_type,
                                change_type="CREATED",
                                snapshot_ddl=None,
                                current_ddl=current_ddl,
                                diff_summary=f"{object_type} was created after snapshot"
                            ))
                        except:
                            pass

        # Count changes by type
        created_count = sum(1 for c in changes if c.change_type == "CREATED")
        modified_count = sum(1 for c in changes if c.change_type == "MODIFIED")
        deleted_count = sum(1 for c in changes if c.change_type == "DELETED")
        unchanged_count = sum(1 for c in changes if c.change_type == "UNCHANGED")

        return SnapshotDiffResponse(
            success=True,
            snapshot_id=snapshot_id,
            total_objects=len(changes),
            created_count=created_count,
            modified_count=modified_count,
            deleted_count=deleted_count,
            unchanged_count=unchanged_count,
            changes=changes
        )

    except HTTPException:
        raise
    except Exception as e:
        return SnapshotDiffResponse(
            success=False,
            error=f"Failed to compute diff: {str(e)}"
        )


@router.post("/rollback/validate", response_model=RollbackValidationResponse)
async def validate_rollback(request: RollbackValidationRequest):
    """Validate if rollback is safe to perform"""
    try:
        if request.snapshot_id not in snapshots:
            raise HTTPException(status_code=404, detail="Snapshot not found")

        snapshot_data = snapshots[request.snapshot_id]
        issues: List[ValidationIssue] = []
        affected_objects = 0

        safe_catalog = quote_identifier(request.catalog)
        safe_schema = quote_identifier(request.schema_name)

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                # Get current tables
                cursor.execute(f"SHOW TABLES IN {safe_catalog}.{safe_schema}")
                current_tables = {row[1]: True for row in cursor.fetchall()}

                tables_to_check = request.tables if request.tables else snapshot_data["tables"]

                for table_name in tables_to_check:
                    affected_objects += 1

                    if table_name in current_tables:
                        # Check if table has dependencies
                        try:
                            safe_table = quote_identifier(table_name)
                            full_table_name = f"{safe_catalog}.{safe_schema}.{safe_table}"

                            # Check for views that depend on this table
                            cursor.execute(f"""
                                SELECT table_name
                                FROM {safe_catalog}.information_schema.views
                                WHERE table_schema = '{request.schema_name}'
                                AND view_definition LIKE '%{table_name}%'
                            """)
                            dependent_views = cursor.fetchall()

                            if dependent_views:
                                issues.append(ValidationIssue(
                                    severity="WARNING",
                                    object_name=table_name,
                                    message=f"Table has {len(dependent_views)} dependent views that may break",
                                    can_proceed=True
                                ))
                        except:
                            # Information schema may not be available
                            pass

                        # Check if table is a Delta table with active readers
                        try:
                            cursor.execute(f"DESCRIBE DETAIL {full_table_name}")
                            detail = cursor.fetchone()
                            if detail:
                                issues.append(ValidationIssue(
                                    severity="INFO",
                                    object_name=table_name,
                                    message="Table will be dropped and recreated from snapshot",
                                    can_proceed=True
                                ))
                        except:
                            pass
                    else:
                        # Table doesn't exist, will be created
                        issues.append(ValidationIssue(
                            severity="INFO",
                            object_name=table_name,
                            message="Table will be created from snapshot",
                            can_proceed=True
                        ))

                # Check for objects that will be dropped
                for current_table in current_tables:
                    if current_table not in snapshot_data["tables"]:
                        affected_objects += 1
                        issues.append(ValidationIssue(
                            severity="WARNING",
                            object_name=current_table,
                            message="Table exists now but not in snapshot - will be DROPPED",
                            can_proceed=True
                        ))

        warnings_count = sum(1 for i in issues if i.severity == "WARNING")
        errors_count = sum(1 for i in issues if i.severity == "ERROR")
        can_rollback = errors_count == 0

        return RollbackValidationResponse(
            success=True,
            can_rollback=can_rollback,
            issues=issues,
            warnings_count=warnings_count,
            errors_count=errors_count,
            affected_objects=affected_objects
        )

    except HTTPException:
        raise
    except Exception as e:
        return RollbackValidationResponse(
            success=False,
            can_rollback=False,
            issues=[],
            warnings_count=0,
            errors_count=0,
            affected_objects=0,
            error=str(e)
        )


@router.post("/rollback/restore/{snapshot_id}", response_model=RestoreSnapshotResponse)
async def restore_snapshot(snapshot_id: str, request: RestoreSnapshotRequest):
    """Restore database objects from a snapshot"""
    try:
        if snapshot_id not in snapshots:
            raise HTTPException(status_code=404, detail="Snapshot not found")

        if request.snapshot_id != snapshot_id:
            raise HTTPException(status_code=400, detail="Snapshot ID mismatch")

        snapshot_data = snapshots[snapshot_id]
        results: List[RestoreResult] = []
        successful = 0
        failed = 0

        safe_catalog = quote_identifier(request.catalog)
        safe_schema = quote_identifier(request.schema_name)

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                # Get current tables
                cursor.execute(f"SHOW TABLES IN {safe_catalog}.{safe_schema}")
                current_tables = {row[1]: True for row in cursor.fetchall()}

                snapshot_objects = {obj["table_name"]: obj for obj in snapshot_data["objects"]}
                tables_to_restore = request.tables if request.tables else list(snapshot_objects.keys())

                # Step 1: Drop objects not in snapshot (if drop_existing=True)
                if request.drop_existing:
                    for current_table in current_tables:
                        if current_table not in snapshot_objects:
                            try:
                                safe_table = quote_identifier(current_table)
                                full_table_name = f"{safe_catalog}.{safe_schema}.{safe_table}"
                                drop_ddl = f"DROP TABLE IF EXISTS {full_table_name}"

                                if not request.dry_run:
                                    cursor.execute(drop_ddl)

                                results.append(RestoreResult(
                                    object_name=current_table,
                                    object_type="TABLE",
                                    action="DROPPED",
                                    status="success",
                                    ddl_executed=drop_ddl if not request.dry_run else None
                                ))
                                successful += 1
                            except Exception as e:
                                results.append(RestoreResult(
                                    object_name=current_table,
                                    object_type="TABLE",
                                    action="DROPPED",
                                    status="error",
                                    error_message=str(e)
                                ))
                                failed += 1

                # Step 2: Restore objects from snapshot
                for table_name in tables_to_restore:
                    if table_name not in snapshot_objects:
                        continue

                    snapshot_obj = snapshot_objects[table_name]
                    safe_table = quote_identifier(table_name)
                    full_table_name = f"{safe_catalog}.{safe_schema}.{safe_table}"

                    try:
                        # Drop existing table/view first
                        drop_ddl = f"DROP {snapshot_obj['object_type']} IF EXISTS {full_table_name}"

                        if not request.dry_run:
                            cursor.execute(drop_ddl)
                            # Execute CREATE statement from snapshot
                            cursor.execute(snapshot_obj["ddl"])

                            # Restore data using time travel if requested and version available
                            if request.restore_data and snapshot_obj.get("version") is not None:
                                try:
                                    restore_data_sql = f"""
                                        INSERT INTO {full_table_name}
                                        SELECT * FROM {full_table_name} VERSION AS OF {snapshot_obj['version']}
                                    """
                                    cursor.execute(restore_data_sql)
                                except:
                                    # Time travel may not be available for all tables
                                    pass

                        action = "CREATED" if table_name not in current_tables else "RESTORED"
                        results.append(RestoreResult(
                            object_name=table_name,
                            object_type=snapshot_obj["object_type"],
                            action=action,
                            status="success",
                            ddl_executed=snapshot_obj["ddl"] if not request.dry_run else None
                        ))
                        successful += 1

                    except Exception as e:
                        results.append(RestoreResult(
                            object_name=table_name,
                            object_type=snapshot_obj["object_type"],
                            action="RESTORED",
                            status="error",
                            error_message=str(e)
                        ))
                        failed += 1

        return RestoreSnapshotResponse(
            success=True,
            snapshot_id=snapshot_id,
            total_actions=len(results),
            successful=successful,
            failed=failed,
            results=results,
            dry_run=request.dry_run
        )

    except HTTPException:
        raise
    except Exception as e:
        return RestoreSnapshotResponse(
            success=False,
            error=f"Failed to restore snapshot: {str(e)}"
        )


@router.delete("/rollback/snapshot/{snapshot_id}", response_model=DeleteSnapshotResponse)
async def delete_snapshot(snapshot_id: str):
    """Delete a snapshot"""
    try:
        if snapshot_id not in snapshots:
            raise HTTPException(status_code=404, detail="Snapshot not found")

        # Remove from in-memory store
        del snapshots[snapshot_id]

        # In production, also delete from persistent storage (Delta Lake, Volume, etc.)

        return DeleteSnapshotResponse(
            success=True,
            message=f"Snapshot {snapshot_id} deleted successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        return DeleteSnapshotResponse(
            success=False,
            error=str(e)
        )
