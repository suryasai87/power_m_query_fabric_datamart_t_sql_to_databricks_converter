"""Query testing endpoints: single test, batch, results, compare results."""
import time
import uuid
import asyncio
from typing import List
from datetime import datetime
from fastapi import APIRouter, HTTPException

from app.config import DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
from app.models import (
    TestQueryRequest, TestQueryResponse,
    BatchTestRequest, BatchTestResponse,
    TestResultsResponse,
    CompareResultsRequest, CompareResultsResponse,
)
from app.state import test_jobs
from app.utils import validate_sql_syntax, quote_identifier
from app.dependencies import get_databricks_connection

router = APIRouter()


def execute_query_with_timeout(query: str, catalog: str, schema: str, timeout_seconds: int) -> TestQueryResponse:
    """Execute a query with timeout and collect metrics"""
    start_time = time.time()

    # Validate syntax first
    syntax_valid, syntax_error = validate_sql_syntax(query)
    if not syntax_valid:
        return TestQueryResponse(
            success=False,
            query=query,
            syntax_valid=False,
            execution_status="error",
            error_message=syntax_error
        )

    try:
        # Validate identifiers to prevent SQL injection
        safe_catalog = quote_identifier(catalog)
        safe_schema = quote_identifier(schema)

        # Add LIMIT to prevent large result sets
        query_trimmed = query.strip().rstrip(';')
        if query_trimmed.upper().startswith('SELECT') and 'LIMIT' not in query_trimmed.upper():
            query_trimmed = f"{query_trimmed} LIMIT 100"

        full_sql = f"USE CATALOG {safe_catalog}; USE SCHEMA {safe_schema}; {query_trimmed}"

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(full_sql)

                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                # Convert rows to dictionaries for sample data
                sample_rows = [dict(zip(columns, row)) for row in rows[:10]]
                row_count = len(rows)

                execution_time_ms = int((time.time() - start_time) * 1000)

                return TestQueryResponse(
                    success=True,
                    query=query,
                    syntax_valid=True,
                    execution_status="success",
                    execution_time_ms=execution_time_ms,
                    row_count=row_count,
                    rows_scanned=row_count,  # Approximation
                    sample_rows=sample_rows
                )

    except Exception as e:
        execution_time_ms = int((time.time() - start_time) * 1000)
        error_msg = str(e)

        # Check if timeout
        if execution_time_ms >= timeout_seconds * 1000:
            status = "timeout"
            error_msg = f"Query execution timed out after {timeout_seconds} seconds"
        else:
            status = "error"

        return TestQueryResponse(
            success=False,
            query=query,
            syntax_valid=True,
            execution_status=status,
            execution_time_ms=execution_time_ms,
            error_message=error_msg
        )


@router.post("/test/query", response_model=TestQueryResponse)
async def test_query(request: TestQueryRequest):
    """Test a single query execution"""
    try:
        return execute_query_with_timeout(
            request.query,
            request.catalog,
            request.schema,
            request.timeout_seconds
        )
    except Exception as e:
        return TestQueryResponse(
            success=False,
            query=request.query,
            syntax_valid=False,
            execution_status="error",
            error_message=f"Test execution failed: {str(e)}"
        )


@router.post("/test/batch", response_model=BatchTestResponse)
async def test_batch_queries(request: BatchTestRequest):
    """Test multiple queries in batch (async)"""
    try:
        job_id = str(uuid.uuid4())

        # Store job info
        test_jobs[job_id] = {
            "status": "running",
            "total": len(request.queries),
            "completed": 0,
            "results": [],
            "started_at": datetime.now().isoformat()
        }

        # Start background task to process queries
        asyncio.create_task(process_batch_queries(
            job_id,
            request.queries,
            request.catalog,
            request.schema,
            request.timeout_seconds
        ))

        return BatchTestResponse(
            success=True,
            job_id=job_id,
            total_queries=len(request.queries),
            message=f"Batch test started with {len(request.queries)} queries"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start batch test: {str(e)}")


async def process_batch_queries(job_id: str, queries: List[str], catalog: str, schema: str, timeout_seconds: int):
    """Background task to process batch queries"""
    try:
        results = []
        for idx, query in enumerate(queries):
            # Execute query
            result = execute_query_with_timeout(query, catalog, schema, timeout_seconds)
            results.append(result)

            # Update job progress
            test_jobs[job_id]["completed"] = idx + 1
            test_jobs[job_id]["results"] = results

        # Mark job as completed
        test_jobs[job_id]["status"] = "completed"
        test_jobs[job_id]["completed_at"] = datetime.now().isoformat()

    except Exception as e:
        test_jobs[job_id]["status"] = "failed"
        test_jobs[job_id]["error"] = str(e)


@router.get("/test/results/{job_id}", response_model=TestResultsResponse)
async def get_test_results(job_id: str):
    """Get test results for a batch job"""
    if job_id not in test_jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = test_jobs[job_id]

    return TestResultsResponse(
        success=True,
        job_id=job_id,
        status=job["status"],
        completed=job["completed"],
        total=job["total"],
        results=job.get("results", [])
    )


@router.post("/test/compare-results", response_model=CompareResultsResponse)
async def compare_query_results(request: CompareResultsRequest):
    """Compare results from source and target queries"""
    try:
        # Execute source query
        source_start = time.time()
        source_result = execute_query_with_timeout(
            request.source_query,
            request.source_catalog,
            request.source_schema,
            60  # 60 second timeout for comparison
        )
        source_execution_time_ms = int((time.time() - source_start) * 1000)

        if not source_result.success:
            return CompareResultsResponse(
                success=False,
                row_count_match=False,
                data_match=False,
                error_message=f"Source query failed: {source_result.error_message}"
            )

        # Execute target query
        target_start = time.time()
        target_result = execute_query_with_timeout(
            request.target_query,
            request.target_catalog,
            request.target_schema,
            60
        )
        target_execution_time_ms = int((time.time() - target_start) * 1000)

        if not target_result.success:
            return CompareResultsResponse(
                success=False,
                row_count_match=False,
                data_match=False,
                source_row_count=source_result.row_count,
                source_execution_time_ms=source_execution_time_ms,
                error_message=f"Target query failed: {target_result.error_message}"
            )

        # Compare row counts
        row_count_match = source_result.row_count == target_result.row_count

        # Compare data (sample rows)
        discrepancies = []
        data_match = True

        if source_result.sample_rows and target_result.sample_rows:
            # Compare up to sample_size rows
            max_compare = min(len(source_result.sample_rows), len(target_result.sample_rows), request.sample_size)

            for i in range(max_compare):
                source_row = source_result.sample_rows[i]
                target_row = target_result.sample_rows[i]

                # Compare row data
                row_diffs = {}
                all_keys = set(source_row.keys()) | set(target_row.keys())

                for key in all_keys:
                    source_val = source_row.get(key)
                    target_val = target_row.get(key)

                    # Simple comparison (could be enhanced for floating point tolerance)
                    if source_val != target_val:
                        row_diffs[key] = {
                            "source": str(source_val),
                            "target": str(target_val)
                        }
                        data_match = False

                if row_diffs:
                    discrepancies.append({
                        "row_index": i,
                        "differences": row_diffs
                    })

        return CompareResultsResponse(
            success=True,
            row_count_match=row_count_match,
            source_row_count=source_result.row_count,
            target_row_count=target_result.row_count,
            data_match=data_match,
            discrepancies=discrepancies if discrepancies else None,
            source_execution_time_ms=source_execution_time_ms,
            target_execution_time_ms=target_execution_time_ms
        )

    except Exception as e:
        return CompareResultsResponse(
            success=False,
            row_count_match=False,
            data_match=False,
            error_message=f"Comparison failed: {str(e)}"
        )
