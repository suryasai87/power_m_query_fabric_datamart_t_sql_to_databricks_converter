"""Migration endpoints: bulk migrate, start/progress/stream/cancel, jobs, history."""
import re
import time
import json
import uuid
import asyncio
import logging
from typing import List
from datetime import datetime
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from databricks import sql

from app.config import (
    DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH, ERROR_LOG_DIRECTORY
)
from app.models import (
    MigrationRequest, MigrationResult, MigrationResponse,
)
from app.state import migration_jobs, migration_locks, active_connections
from app.dependencies import OPENAI_AVAILABLE, openai, get_databricks_connection, get_llm_client

logger = logging.getLogger(__name__)

# Import migration progress helpers
try:
    from migration_progress import (
        get_migration_lock, initialize_migration_job, update_migration_progress,
        add_migration_log, add_object_result, complete_migration_job, generate_sse_events
    )
    MIGRATION_PROGRESS_AVAILABLE = True
except ImportError:
    MIGRATION_PROGRESS_AVAILABLE = False

router = APIRouter()


@router.post("/migrate/bulk", response_model=MigrationResponse)
async def bulk_migrate(request: MigrationRequest):
    """Migrate extracted objects to Databricks SQL with AI translation"""
    results: List[MigrationResult] = []
    successful = 0
    failed = 0
    skipped = 0
    error_log_entries = []

    try:
        if not OPENAI_AVAILABLE or openai is None:
            return MigrationResponse(
                success=False,
                total_objects=0,
                successful=0,
                failed=0,
                skipped=0,
                results=[],
                error_log_path=None
            )

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                # Read inventory from volume path
                try:
                    cursor.execute(f"SELECT * FROM read_files('{request.inventory_path}/inventory.json')")
                    inventory_result = cursor.fetchone()
                    inventory_data = json.loads(inventory_result[0]) if inventory_result else {}
                except:
                    # Try workspace path
                    inventory_data = {"inventory": {"tables": [], "views": [], "stored_procedures": []}}

                inventory = inventory_data.get("inventory", {})
                source_type = inventory_data.get("source_type", request.source_type)

                # Initialize OpenAI client for translation
                client = get_llm_client()

                all_objects = []

                # Collect tables
                for table in inventory.get("tables", []):
                    all_objects.append({
                        "type": "TABLE",
                        "schema": table.get("schema", ""),
                        "name": table.get("name", ""),
                        "ddl": table.get("ddl", f"-- DDL for {table.get('name', 'unknown')}")
                    })

                # Collect views
                for view in inventory.get("views", []):
                    all_objects.append({
                        "type": "VIEW",
                        "schema": view.get("schema", ""),
                        "name": view.get("name", ""),
                        "definition": view.get("definition", "")
                    })

                # Collect stored procedures
                for proc in inventory.get("stored_procedures", []):
                    all_objects.append({
                        "type": "PROCEDURE",
                        "schema": proc.get("schema", ""),
                        "name": proc.get("name", ""),
                        "definition": proc.get("definition", "")
                    })

                # Process each object
                for obj in all_objects:
                    start_time = time.time()
                    obj_name = f"{obj['schema']}.{obj['name']}"
                    source_sql = obj.get("ddl", obj.get("definition", ""))

                    if not source_sql or source_sql.strip() == "":
                        skipped += 1
                        results.append(MigrationResult(
                            object_name=obj_name,
                            object_type=obj["type"],
                            source_sql="",
                            target_sql="",
                            status="skipped",
                            error_message="No source SQL available"
                        ))
                        continue

                    try:
                        # Translate using AI
                        system_prompt = f"""You are an expert SQL translator. Convert the following {source_type} SQL to Databricks SQL.
Target catalog: {request.target_catalog}
Target schema: {request.target_schema}
Only output the converted SQL, no explanations."""

                        response = client.chat.completions.create(
                            model=request.model_id,
                            messages=[
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": source_sql}
                            ],
                            max_tokens=2000,
                            temperature=0.1
                        )

                        target_sql = response.choices[0].message.content.strip()

                        # Clean up the SQL
                        if target_sql.startswith("```"):
                            target_sql = re.sub(r'^```\w*\n?', '', target_sql)
                            target_sql = re.sub(r'\n?```$', '', target_sql)

                        # Test execution with LIMIT 1
                        if not request.dry_run and obj["type"] in ["TABLE", "VIEW"]:
                            try:
                                # For DDL, just create it
                                if "CREATE" in target_sql.upper():
                                    cursor.execute(target_sql)
                                else:
                                    # For queries, add LIMIT 1
                                    test_sql = f"{target_sql.rstrip(';')} LIMIT 1"
                                    cursor.execute(test_sql)

                                successful += 1
                                results.append(MigrationResult(
                                    object_name=obj_name,
                                    object_type=obj["type"],
                                    source_sql=source_sql,
                                    target_sql=target_sql,
                                    status="success",
                                    execution_time_ms=int((time.time() - start_time) * 1000)
                                ))
                            except Exception as exec_error:
                                failed += 1
                                error_msg = str(exec_error)
                                results.append(MigrationResult(
                                    object_name=obj_name,
                                    object_type=obj["type"],
                                    source_sql=source_sql,
                                    target_sql=target_sql,
                                    status="error",
                                    error_message=error_msg,
                                    execution_time_ms=int((time.time() - start_time) * 1000)
                                ))
                                error_log_entries.append({
                                    "object_name": obj_name,
                                    "object_type": obj["type"],
                                    "source_sql": source_sql,
                                    "target_sql": target_sql,
                                    "error": error_msg,
                                    "timestamp": datetime.now().isoformat()
                                })
                        else:
                            # Dry run - just translate
                            successful += 1
                            results.append(MigrationResult(
                                object_name=obj_name,
                                object_type=obj["type"],
                                source_sql=source_sql,
                                target_sql=target_sql,
                                status="success" if request.dry_run else "pending",
                                execution_time_ms=int((time.time() - start_time) * 1000)
                            ))

                    except Exception as translate_error:
                        failed += 1
                        results.append(MigrationResult(
                            object_name=obj_name,
                            object_type=obj["type"],
                            source_sql=source_sql,
                            target_sql="",
                            status="error",
                            error_message=str(translate_error)
                        ))
                        error_log_entries.append({
                            "object_name": obj_name,
                            "object_type": obj["type"],
                            "source_sql": source_sql,
                            "target_sql": "",
                            "error": str(translate_error),
                            "timestamp": datetime.now().isoformat()
                        })

                # Write error log if there are errors
                error_log_path = None
                if error_log_entries:
                    try:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        error_log_path = f"/Workspace/Users/dw_migration/{ERROR_LOG_DIRECTORY}/migration_errors_{timestamp}.json"
                    except:
                        pass

                return MigrationResponse(
                    success=True,
                    total_objects=len(all_objects),
                    successful=successful,
                    failed=failed,
                    skipped=skipped,
                    results=results,
                    error_log_path=error_log_path
                )

    except Exception as e:
        return MigrationResponse(
            success=False,
            total_objects=0,
            successful=0,
            failed=0,
            skipped=0,
            results=[],
            error_log_path=None
        )


# ============================================
# MIGRATION PROGRESS TRACKING ENDPOINTS
# ============================================

@router.post("/migrate/start")
async def start_migration(request: MigrationRequest):
    """
    Start a migration job and return job_id for tracking progress
    This endpoint initiates the migration but returns immediately
    """
    try:
        # Generate unique job ID
        job_id = str(uuid.uuid4())

        # Get inventory data to count total objects
        total_objects = 0
        try:
            with get_databricks_connection() as connection:
                with connection.cursor() as cursor:
                    try:
                        cursor.execute(f"SELECT * FROM read_files('{request.inventory_path}/inventory.json')")
                        inventory_result = cursor.fetchone()
                        inventory_data = json.loads(inventory_result[0]) if inventory_result else {}
                    except:
                        inventory_data = {"inventory": {"tables": [], "views": [], "stored_procedures": []}}

                    inventory = inventory_data.get("inventory", {})
                    total_objects = (
                        len(inventory.get("tables", [])) +
                        len(inventory.get("views", [])) +
                        len(inventory.get("stored_procedures", []))
                    )
        except:
            total_objects = 0

        # Initialize job state
        lock = get_migration_lock(job_id, migration_locks)
        async with lock:
            initialize_migration_job(
                job_id, migration_jobs, total_objects,
                request.source_type, request.target_catalog, request.target_schema
            )
            add_migration_log(job_id, migration_jobs, "info",
                            f"Migration job started: {total_objects} objects to migrate")

        # Start migration in background
        asyncio.create_task(run_migration_job(job_id, request))

        return {
            "success": True,
            "job_id": job_id,
            "total_objects": total_objects,
            "message": "Migration job started successfully"
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.get("/migrate/progress/{job_id}")
async def get_migration_progress(job_id: str):
    """Get current migration job progress (one-time snapshot)"""
    lock = get_migration_lock(job_id, migration_locks)
    async with lock:
        if job_id not in migration_jobs:
            raise HTTPException(status_code=404, detail="Migration job not found")

        job = migration_jobs[job_id].copy()
        return job


@router.get("/migrate/stream/{job_id}")
async def stream_migration_progress(job_id: str):
    """Stream migration progress using Server-Sent Events (SSE)"""
    return StreamingResponse(
        generate_sse_events(job_id, migration_jobs, migration_locks),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable nginx buffering
        }
    )


@router.post("/migrate/cancel/{job_id}")
async def cancel_migration(job_id: str):
    """Cancel a running migration job"""
    lock = get_migration_lock(job_id, migration_locks)
    async with lock:
        if job_id not in migration_jobs:
            raise HTTPException(status_code=404, detail="Migration job not found")

        if migration_jobs[job_id]["status"] == "running":
            migration_jobs[job_id]["status"] = "cancelled"
            add_migration_log(job_id, migration_jobs, "warning", "Migration cancelled by user")
            complete_migration_job(job_id, migration_jobs, "cancelled")
            return {"success": True, "message": "Migration job cancelled"}
        else:
            return {
                "success": False,
                "message": f"Cannot cancel job with status: {migration_jobs[job_id]['status']}"
            }


@router.delete("/migrate/jobs/{job_id}")
async def delete_migration_job(job_id: str):
    """Delete a migration job from memory"""
    lock = get_migration_lock(job_id, migration_locks)
    async with lock:
        if job_id in migration_jobs:
            del migration_jobs[job_id]
        if job_id in migration_locks:
            del migration_locks[job_id]
    return {"success": True, "message": "Migration job deleted"}


@router.get("/migrate/jobs")
async def list_migration_jobs():
    """List all migration jobs"""
    jobs = []
    for job_id in list(migration_jobs.keys()):
        lock = get_migration_lock(job_id, migration_locks)
        async with lock:
            if job_id in migration_jobs:
                job = migration_jobs[job_id].copy()
                # Don't send full logs and results in list view
                job["log_count"] = len(job.get("logs", []))
                job["result_count"] = len(job.get("object_results", []))
                job.pop("logs", None)
                job.pop("object_results", None)
                jobs.append(job)
    return {"jobs": jobs}


@router.get("/migrate/history")
async def get_migration_history():
    """Get migration history from workspace"""
    try:
        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                # List files in error log directory
                try:
                    cursor.execute(f"""
                        SELECT * FROM list_files('/Workspace/Users/dw_migration/{ERROR_LOG_DIRECTORY}/')
                    """)
                    files = cursor.fetchall()
                    return {"history": [{"file": f[0], "size": f[1]} for f in files]}
                except:
                    return {"history": [], "message": "No migration history found"}
    except Exception as e:
        return {"history": [], "error": str(e)}


async def run_migration_job(job_id: str, request: MigrationRequest):
    """
    Background task to run the actual migration
    This updates the job state as it progresses
    """
    try:
        lock = get_migration_lock(job_id, migration_locks)

        if not OPENAI_AVAILABLE or openai is None:
            async with lock:
                add_migration_log(job_id, migration_jobs, "error", "OpenAI library not available")
                complete_migration_job(job_id, migration_jobs, "failed")
            return

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                # Read inventory from volume path
                try:
                    cursor.execute(f"SELECT * FROM read_files('{request.inventory_path}/inventory.json')")
                    inventory_result = cursor.fetchone()
                    inventory_data = json.loads(inventory_result[0]) if inventory_result else {}
                except:
                    inventory_data = {"inventory": {"tables": [], "views": [], "stored_procedures": []}}

                inventory = inventory_data.get("inventory", {})
                source_type = inventory_data.get("source_type", request.source_type)

                # Initialize OpenAI client for translation
                client = get_llm_client()

                all_objects = []

                # Collect tables
                for table in inventory.get("tables", []):
                    all_objects.append({
                        "type": "TABLE",
                        "schema": table.get("schema", ""),
                        "name": table.get("name", ""),
                        "ddl": table.get("ddl", f"-- DDL for {table.get('name', 'unknown')}")
                    })

                # Collect views
                for view in inventory.get("views", []):
                    all_objects.append({
                        "type": "VIEW",
                        "schema": view.get("schema", ""),
                        "name": view.get("name", ""),
                        "definition": view.get("definition", "")
                    })

                # Collect stored procedures
                for proc in inventory.get("stored_procedures", []):
                    all_objects.append({
                        "type": "PROCEDURE",
                        "schema": proc.get("schema", ""),
                        "name": proc.get("name", ""),
                        "definition": proc.get("definition", "")
                    })

                # Process each object
                for idx, obj in enumerate(all_objects):
                    # Check if job was cancelled
                    async with lock:
                        if migration_jobs[job_id]["status"] == "cancelled":
                            add_migration_log(job_id, migration_jobs, "warning",
                                            "Migration cancelled, stopping processing")
                            return

                    start_time = time.time()
                    obj_name = f"{obj['schema']}.{obj['name']}"
                    source_sql = obj.get("ddl", obj.get("definition", ""))

                    # Update current object
                    async with lock:
                        update_migration_progress(job_id, migration_jobs, current_object=obj_name)
                        add_migration_log(job_id, migration_jobs, "info",
                                        f"Processing {obj['type']} {obj_name} ({idx+1}/{len(all_objects)})")

                    if not source_sql or source_sql.strip() == "":
                        async with lock:
                            update_migration_progress(
                                job_id, migration_jobs,
                                completed_objects=migration_jobs[job_id]["completed_objects"] + 1
                            )
                            add_object_result(job_id, migration_jobs, obj_name, obj["type"], "skipped",
                                            error="No source SQL available")
                            add_migration_log(job_id, migration_jobs, "warning",
                                            f"Skipped {obj_name}: No source SQL available")
                        continue

                    try:
                        # Translate using AI
                        system_prompt = f"""You are an expert SQL translator. Convert the following {source_type} SQL to Databricks SQL.
Target catalog: {request.target_catalog}
Target schema: {request.target_schema}
Only output the converted SQL, no explanations."""

                        response = client.chat.completions.create(
                            model=request.model_id,
                            messages=[
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": source_sql}
                            ],
                            max_tokens=2000,
                            temperature=0.1
                        )

                        target_sql = response.choices[0].message.content.strip()

                        # Clean up the SQL
                        if target_sql.startswith("```"):
                            target_sql = re.sub(r'^```\w*\n?', '', target_sql)
                            target_sql = re.sub(r'\n?```$', '', target_sql)

                        execution_time_ms = int((time.time() - start_time) * 1000)

                        # Test execution with LIMIT 1 or execute
                        if not request.dry_run and obj["type"] in ["TABLE", "VIEW"]:
                            try:
                                if "CREATE" in target_sql.upper():
                                    cursor.execute(target_sql)
                                else:
                                    test_sql = f"{target_sql.rstrip(';')} LIMIT 1"
                                    cursor.execute(test_sql)

                                async with lock:
                                    update_migration_progress(
                                        job_id, migration_jobs,
                                        completed_objects=migration_jobs[job_id]["completed_objects"] + 1
                                    )
                                    add_object_result(job_id, migration_jobs, obj_name, obj["type"],
                                                    "success", execution_time_ms=execution_time_ms)
                                    add_migration_log(job_id, migration_jobs, "info",
                                                    f"Successfully migrated {obj_name} in {execution_time_ms}ms")
                            except Exception as exec_error:
                                error_msg = str(exec_error)
                                async with lock:
                                    update_migration_progress(
                                        job_id, migration_jobs,
                                        failed_objects=migration_jobs[job_id]["failed_objects"] + 1
                                    )
                                    add_object_result(job_id, migration_jobs, obj_name, obj["type"],
                                                    "error", error=error_msg, execution_time_ms=execution_time_ms)
                                    add_migration_log(job_id, migration_jobs, "error",
                                                    f"Failed to migrate {obj_name}: {error_msg}")
                        else:
                            # Dry run - just translate
                            async with lock:
                                update_migration_progress(
                                    job_id, migration_jobs,
                                    completed_objects=migration_jobs[job_id]["completed_objects"] + 1
                                )
                                add_object_result(job_id, migration_jobs, obj_name, obj["type"],
                                                "success", execution_time_ms=execution_time_ms)
                                add_migration_log(job_id, migration_jobs, "info",
                                                f"Translated {obj_name} in {execution_time_ms}ms (dry run)")

                    except Exception as translate_error:
                        async with lock:
                            update_migration_progress(
                                job_id, migration_jobs,
                                failed_objects=migration_jobs[job_id]["failed_objects"] + 1
                            )
                            add_object_result(job_id, migration_jobs, obj_name, obj["type"],
                                            "error", error=str(translate_error))
                            add_migration_log(job_id, migration_jobs, "error",
                                            f"Translation error for {obj_name}: {str(translate_error)}")

                # Mark job as complete
                async with lock:
                    completed = migration_jobs[job_id]["completed_objects"]
                    failed = migration_jobs[job_id]["failed_objects"]
                    add_migration_log(job_id, migration_jobs, "info",
                                    f"Migration completed: {completed} successful, {failed} failed")
                    complete_migration_job(job_id, migration_jobs, "completed")

    except Exception as e:
        logger.error(f"Migration job {job_id} failed with error: {str(e)}")
        async with lock:
            add_migration_log(job_id, migration_jobs, "error", f"Migration failed: {str(e)}")
            complete_migration_job(job_id, migration_jobs, "failed")
