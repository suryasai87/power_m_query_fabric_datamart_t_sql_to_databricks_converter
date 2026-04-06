"""Scheduling endpoints: CRUD, run-now, execution history."""
import time
import uuid
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException

from app.models import (
    MigrationRequest, MigrationSchedule,
    ScheduleJobExecution, JobStatus,
    CreateScheduleRequest, UpdateScheduleRequest,
    ScheduleResponse, ScheduleListResponse,
    JobExecutionResponse, JobHistoryResponse,
)
from app.state import scheduled_jobs, job_executions

router = APIRouter()


@router.post("/schedule/create", response_model=ScheduleResponse)
async def create_schedule(request: CreateScheduleRequest):
    """Create a new scheduled migration job"""
    try:
        schedule = request.schedule

        # Generate job ID if not provided
        if not schedule.job_id:
            schedule.job_id = str(uuid.uuid4())

        # Set timestamps
        schedule.created_at = datetime.now().isoformat()
        schedule.updated_at = datetime.now().isoformat()

        # Validate dependencies
        if schedule.dependencies:
            for dep_id in schedule.dependencies:
                if dep_id not in scheduled_jobs:
                    return ScheduleResponse(
                        success=False,
                        error=f"Dependency job {dep_id} not found"
                    )

        # Store schedule
        scheduled_jobs[schedule.job_id] = schedule

        return ScheduleResponse(
            success=True,
            schedule=schedule
        )
    except Exception as e:
        return ScheduleResponse(
            success=False,
            error=str(e)
        )


@router.get("/schedule/list", response_model=ScheduleListResponse)
async def list_schedules():
    """List all scheduled migration jobs"""
    try:
        schedules = list(scheduled_jobs.values())
        return ScheduleListResponse(
            success=True,
            schedules=schedules,
            total=len(schedules)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schedule/{job_id}", response_model=ScheduleResponse)
async def get_schedule(job_id: str):
    """Get details of a specific scheduled job"""
    try:
        if job_id not in scheduled_jobs:
            return ScheduleResponse(
                success=False,
                error=f"Schedule with job_id {job_id} not found"
            )

        return ScheduleResponse(
            success=True,
            schedule=scheduled_jobs[job_id]
        )
    except Exception as e:
        return ScheduleResponse(
            success=False,
            error=str(e)
        )


@router.put("/schedule/{job_id}", response_model=ScheduleResponse)
async def update_schedule(job_id: str, request: UpdateScheduleRequest):
    """Update an existing scheduled job"""
    try:
        if job_id not in scheduled_jobs:
            return ScheduleResponse(
                success=False,
                error=f"Schedule with job_id {job_id} not found"
            )

        schedule = scheduled_jobs[job_id]

        # Update fields if provided
        if request.job_name is not None:
            schedule.job_name = request.job_name
        if request.description is not None:
            schedule.description = request.description
        if request.frequency is not None:
            schedule.frequency = request.frequency
        if request.cron_expression is not None:
            schedule.cron_expression = request.cron_expression
        if request.start_date is not None:
            schedule.start_date = request.start_date
        if request.end_date is not None:
            schedule.end_date = request.end_date
        if request.enabled is not None:
            schedule.enabled = request.enabled
        if request.dependencies is not None:
            # Validate dependencies
            for dep_id in request.dependencies:
                if dep_id not in scheduled_jobs:
                    return ScheduleResponse(
                        success=False,
                        error=f"Dependency job {dep_id} not found"
                    )
            schedule.dependencies = request.dependencies
        if request.notification_emails is not None:
            schedule.notification_emails = request.notification_emails

        schedule.updated_at = datetime.now().isoformat()

        return ScheduleResponse(
            success=True,
            schedule=schedule
        )
    except Exception as e:
        return ScheduleResponse(
            success=False,
            error=str(e)
        )


@router.delete("/schedule/{job_id}")
async def delete_schedule(job_id: str):
    """Cancel/delete a scheduled job"""
    try:
        if job_id not in scheduled_jobs:
            return {"success": False, "error": f"Schedule with job_id {job_id} not found"}

        del scheduled_jobs[job_id]

        return {"success": True, "message": f"Schedule {job_id} deleted successfully"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/schedule/{job_id}/run-now", response_model=JobExecutionResponse)
async def run_schedule_now(job_id: str):
    """Trigger immediate execution of a scheduled job"""
    try:
        if job_id not in scheduled_jobs:
            return JobExecutionResponse(
                success=False,
                error=f"Schedule with job_id {job_id} not found"
            )

        schedule = scheduled_jobs[job_id]

        # Check dependencies
        if schedule.dependencies:
            for dep_id in schedule.dependencies:
                # Check if dependency has completed successfully
                dep_executions = [e for e in job_executions.values()
                                if e.job_id == dep_id and e.status == JobStatus.COMPLETED]
                if not dep_executions:
                    return JobExecutionResponse(
                        success=False,
                        error=f"Dependency job {dep_id} has not completed successfully"
                    )

        # Create execution record
        execution_id = str(uuid.uuid4())
        execution = ScheduleJobExecution(
            execution_id=execution_id,
            job_id=job_id,
            job_name=schedule.job_name,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
            triggered_by="manual"
        )

        job_executions[execution_id] = execution

        # Execute migration asynchronously
        try:
            start_time = time.time()

            # Import here to avoid circular imports
            from app.routers.migration import bulk_migrate

            # Build migration request
            migration_request = MigrationRequest(
                inventory_path=schedule.inventory_path or "",
                target_catalog=schedule.target_catalog,
                target_schema=schedule.target_schema,
                source_type=schedule.source_type,
                model_id=schedule.model_id,
                dry_run=False
            )

            # Run migration
            result = await bulk_migrate(migration_request)

            # Update execution record
            execution.completed_at = datetime.now().isoformat()
            execution.duration_seconds = time.time() - start_time
            execution.objects_migrated = result.successful if result.success else 0
            execution.objects_failed = result.failed if result.success else 0
            execution.status = JobStatus.COMPLETED if result.success else JobStatus.FAILED
            execution.error_message = None if result.success else "Migration failed"

        except Exception as e:
            execution.completed_at = datetime.now().isoformat()
            execution.duration_seconds = time.time() - start_time
            execution.status = JobStatus.FAILED
            execution.error_message = str(e)

        return JobExecutionResponse(
            success=True,
            execution=execution
        )
    except Exception as e:
        return JobExecutionResponse(
            success=False,
            error=str(e)
        )


@router.get("/schedule/executions/history", response_model=JobHistoryResponse)
async def get_job_history(job_id: Optional[str] = None, limit: int = 50):
    """Get execution history for all jobs or a specific job"""
    try:
        executions = list(job_executions.values())

        # Filter by job_id if provided
        if job_id:
            executions = [e for e in executions if e.job_id == job_id]

        # Sort by started_at descending
        executions.sort(key=lambda x: x.started_at, reverse=True)

        # Limit results
        executions = executions[:limit]

        return JobHistoryResponse(
            success=True,
            executions=executions,
            total=len(executions)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schedule/executions/{execution_id}", response_model=JobExecutionResponse)
async def get_execution_details(execution_id: str):
    """Get details of a specific job execution"""
    try:
        if execution_id not in job_executions:
            return JobExecutionResponse(
                success=False,
                error=f"Execution with id {execution_id} not found"
            )

        return JobExecutionResponse(
            success=True,
            execution=job_executions[execution_id]
        )
    except Exception as e:
        return JobExecutionResponse(
            success=False,
            error=str(e)
        )
