"""Pydantic models and Enums for request/response schemas."""
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from enum import Enum


# ============================================
# TRANSLATION / EXECUTE / DDL MODELS
# ============================================

class TranslateSqlRequest(BaseModel):
    sourceSystem: str
    sourceSql: str
    modelId: Optional[str] = "databricks-llama-4-maverick"

class TranslateSqlResponse(BaseModel):
    success: bool
    translatedSql: str
    error: Optional[str] = None
    warnings: Optional[List[str]] = None
    modelUsed: Optional[str] = None
    promptTokens: Optional[int] = None
    completionTokens: Optional[int] = None
    totalTokens: Optional[int] = None
    estimatedCost: Optional[float] = None
    executionTimeMs: Optional[int] = None

class ExecuteSqlRequest(BaseModel):
    sql: str
    catalog: Optional[str] = "main"
    schema: Optional[str] = "default"

class ExecuteSqlResponse(BaseModel):
    success: bool
    result: Optional[Any] = None
    rowCount: Optional[int] = None
    executionTime: Optional[float] = None
    error: Optional[str] = None

class ConvertDdlRequest(BaseModel):
    sourceSystem: str
    sourceDdl: str
    targetCatalog: str
    targetSchema: str
    executeImmediately: Optional[bool] = False

class ConvertDdlResponse(BaseModel):
    success: bool
    convertedDdl: str
    executed: Optional[bool] = False
    error: Optional[str] = None
    warnings: Optional[List[str]] = None


# ============================================
# CATALOG / TABLE MODELS
# ============================================

class CatalogSchemaResponse(BaseModel):
    catalogs: List[str]
    schemas: Dict[str, List[str]]

class TableInfo(BaseModel):
    catalog: str
    schema_name: str
    table: str
    columns: List[str]


# ============================================
# SUGGESTION / GENERATION MODELS
# ============================================

class BusinessLogicSuggestionRequest(BaseModel):
    catalog: str
    schema_name: str
    table: str
    columns: List[str]
    model_id: str = "databricks-llama-4-maverick"
    additional_tables: Optional[List[TableInfo]] = None

class JoinConditionSuggestionRequest(BaseModel):
    tables: List[TableInfo]
    model_id: str = "databricks-llama-4-maverick"

class GenerateSqlRequest(BaseModel):
    tables: List[TableInfo]
    business_logic: str
    model_id: str = "databricks-llama-4-maverick"
    join_conditions: Optional[str] = None


# ============================================
# CONNECT AND MIGRATE MODELS
# ============================================

class SourceConnectionRequest(BaseModel):
    source_type: str  # oracle, snowflake, sqlserver, teradata, netezza, synapse, redshift, mysql
    host: str
    port: int
    database: str
    username: str
    password: str
    additional_params: Optional[Dict[str, str]] = None

class SourceConnectionResponse(BaseModel):
    success: bool
    connection_id: Optional[str] = None
    message: str
    error: Optional[str] = None

class MetadataInventory(BaseModel):
    databases: List[str]
    schemas: List[Dict[str, Any]]
    tables: List[Dict[str, Any]]
    views: List[Dict[str, Any]]
    stored_procedures: List[Dict[str, Any]]
    functions: List[Dict[str, Any]]

class ExtractInventoryRequest(BaseModel):
    connection_id: str
    source_type: str
    include_ddl: bool = True
    include_sample_data: bool = False

class ExtractInventoryResponse(BaseModel):
    success: bool
    inventory: Optional[MetadataInventory] = None
    volume_path: Optional[str] = None
    objects_extracted: int = 0
    error: Optional[str] = None

class MigrationRequest(BaseModel):
    inventory_path: str
    target_catalog: str
    target_schema: str
    source_type: str
    model_id: str = "databricks-llama-4-maverick"
    dry_run: bool = True

class MigrationResult(BaseModel):
    object_name: str
    object_type: str
    source_sql: str
    target_sql: str
    status: str  # success, error, skipped
    error_message: Optional[str] = None
    execution_time_ms: Optional[int] = None

class MigrationResponse(BaseModel):
    success: bool
    total_objects: int
    successful: int
    failed: int
    skipped: int
    results: List[MigrationResult]
    error_log_path: Optional[str] = None


# ============================================
# QUERY TESTING MODELS
# ============================================

class TestQueryRequest(BaseModel):
    query: str
    catalog: Optional[str] = "main"
    schema: Optional[str] = "default"
    timeout_seconds: Optional[int] = 30

class TestQueryResponse(BaseModel):
    success: bool
    query: str
    syntax_valid: bool
    execution_status: str  # success, error, timeout
    execution_time_ms: Optional[int] = None
    row_count: Optional[int] = None
    rows_scanned: Optional[int] = None
    sample_rows: Optional[List[Dict[str, Any]]] = None
    error_message: Optional[str] = None

class BatchTestRequest(BaseModel):
    queries: List[str]
    catalog: Optional[str] = "main"
    schema: Optional[str] = "default"
    timeout_seconds: Optional[int] = 30

class BatchTestResponse(BaseModel):
    success: bool
    job_id: str
    total_queries: int
    message: str

class TestResultsResponse(BaseModel):
    success: bool
    job_id: str
    status: str  # running, completed, failed
    completed: int
    total: int
    results: List[TestQueryResponse]

class CompareResultsRequest(BaseModel):
    source_query: str
    target_query: str
    source_catalog: Optional[str] = "main"
    source_schema: Optional[str] = "default"
    target_catalog: Optional[str] = "main"
    target_schema: Optional[str] = "default"
    sample_size: Optional[int] = 100

class CompareResultsResponse(BaseModel):
    success: bool
    row_count_match: bool
    source_row_count: Optional[int] = None
    target_row_count: Optional[int] = None
    data_match: bool
    discrepancies: Optional[List[Dict[str, Any]]] = None
    source_execution_time_ms: Optional[int] = None
    target_execution_time_ms: Optional[int] = None
    error_message: Optional[str] = None


# ============================================
# COST ESTIMATION MODELS
# ============================================

class MigrationCostEstimateRequest(BaseModel):
    num_tables: int
    num_views: int
    num_procedures: int
    total_rows: Optional[int] = 0
    data_size_gb: Optional[float] = 0.0
    model_id: str = "databricks-llama-4-maverick"
    avg_sql_complexity: Optional[str] = "medium"  # low, medium, high
    source_type: str

class StorageCostEstimateRequest(BaseModel):
    data_size_gb: float
    months: int = 12

class ComputeCostEstimateRequest(BaseModel):
    warehouse_size: str = "X-Small"  # X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large
    estimated_hours: float

class CostComparisonRequest(BaseModel):
    migration_request: MigrationCostEstimateRequest
    storage_months: int = 12
    compute_hours_monthly: float = 100.0

class CostBreakdown(BaseModel):
    llm_translation: float
    compute_migration: float
    storage_annual: float
    network_transfer: float
    total: float

class MigrationCostEstimateResponse(BaseModel):
    success: bool
    breakdown: Optional[CostBreakdown] = None
    estimated_duration_hours: Optional[float] = None
    details: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# ============================================
# ROLLBACK MODELS
# ============================================

class SnapshotObjectInfo(BaseModel):
    catalog: str
    schema_name: str
    table_name: str
    object_type: str  # TABLE, VIEW
    ddl: str
    version: Optional[int] = None  # Delta Lake version if available

class CreateSnapshotRequest(BaseModel):
    catalog: str
    schema_name: str
    description: str
    tables: Optional[List[str]] = None  # If None, snapshot all tables
    include_data: bool = False  # Use Delta Lake time travel for data snapshot
    auto_snapshot: bool = False  # Automatically created before migration

class CreateSnapshotResponse(BaseModel):
    success: bool
    snapshot_id: Optional[str] = None
    created_at: Optional[str] = None
    num_objects: Optional[int] = None
    snapshot_path: Optional[str] = None
    error: Optional[str] = None

class SnapshotInfo(BaseModel):
    snapshot_id: str
    catalog: str
    schema_name: str
    description: str
    created_at: str
    num_objects: int
    tables: List[str]
    include_data: bool
    auto_snapshot: bool
    snapshot_path: str
    created_by: Optional[str] = None

class ListSnapshotsResponse(BaseModel):
    success: bool
    snapshots: List[SnapshotInfo]
    error: Optional[str] = None

class DiffObjectChange(BaseModel):
    object_name: str
    object_type: str
    change_type: str  # CREATED, MODIFIED, DELETED, UNCHANGED
    snapshot_ddl: Optional[str] = None
    current_ddl: Optional[str] = None
    diff_summary: Optional[str] = None

class SnapshotDiffResponse(BaseModel):
    success: bool
    snapshot_id: Optional[str] = None
    total_objects: Optional[int] = None
    created_count: Optional[int] = None
    modified_count: Optional[int] = None
    deleted_count: Optional[int] = None
    unchanged_count: Optional[int] = None
    changes: Optional[List[DiffObjectChange]] = None
    error: Optional[str] = None

class RestoreSnapshotRequest(BaseModel):
    snapshot_id: str
    catalog: str
    schema_name: str
    tables: Optional[List[str]] = None  # If None, restore all tables
    drop_existing: bool = True  # Drop objects not in snapshot
    restore_data: bool = False  # Use Delta Lake time travel to restore data
    dry_run: bool = True

class RestoreResult(BaseModel):
    object_name: str
    object_type: str
    action: str  # CREATED, DROPPED, RESTORED, SKIPPED
    status: str  # success, error
    ddl_executed: Optional[str] = None
    error_message: Optional[str] = None

class RestoreSnapshotResponse(BaseModel):
    success: bool
    snapshot_id: Optional[str] = None
    total_actions: Optional[int] = None
    successful: Optional[int] = None
    failed: Optional[int] = None
    results: Optional[List[RestoreResult]] = None
    dry_run: Optional[bool] = None
    error: Optional[str] = None

class DeleteSnapshotResponse(BaseModel):
    success: bool
    message: Optional[str] = None
    error: Optional[str] = None

class RollbackValidationRequest(BaseModel):
    snapshot_id: str
    catalog: str
    schema_name: str
    tables: Optional[List[str]] = None

class ValidationIssue(BaseModel):
    severity: str  # WARNING, ERROR, INFO
    object_name: str
    message: str
    can_proceed: bool

class RollbackValidationResponse(BaseModel):
    success: bool
    can_rollback: bool
    issues: List[ValidationIssue]
    warnings_count: int
    errors_count: int
    affected_objects: int
    error: Optional[str] = None


# ============================================
# SCHEDULING MODELS
# ============================================

class ScheduleFrequency(str, Enum):
    ONCE = "once"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CRON = "cron"

class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class MigrationSchedule(BaseModel):
    job_id: Optional[str] = None
    job_name: str
    description: Optional[str] = None
    source_type: str
    source_connection_id: Optional[str] = None
    inventory_path: Optional[str] = None
    target_catalog: str
    target_schema: str
    model_id: str = "databricks-llama-4-maverick"
    frequency: ScheduleFrequency
    cron_expression: Optional[str] = None
    start_date: str
    end_date: Optional[str] = None
    enabled: bool = True
    dependencies: Optional[List[str]] = None  # List of job_ids that must complete first
    notification_emails: Optional[List[str]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    created_by: Optional[str] = None

class ScheduleJobExecution(BaseModel):
    execution_id: str
    job_id: str
    job_name: str
    status: JobStatus
    started_at: str
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    objects_migrated: Optional[int] = None
    objects_failed: Optional[int] = None
    error_message: Optional[str] = None
    triggered_by: str = "scheduled"  # scheduled, manual, dependency

class CreateScheduleRequest(BaseModel):
    schedule: MigrationSchedule

class UpdateScheduleRequest(BaseModel):
    job_name: Optional[str] = None
    description: Optional[str] = None
    frequency: Optional[ScheduleFrequency] = None
    cron_expression: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    enabled: Optional[bool] = None
    dependencies: Optional[List[str]] = None
    notification_emails: Optional[List[str]] = None

class ScheduleResponse(BaseModel):
    success: bool
    schedule: Optional[MigrationSchedule] = None
    error: Optional[str] = None

class ScheduleListResponse(BaseModel):
    success: bool
    schedules: List[MigrationSchedule]
    total: int

class JobExecutionResponse(BaseModel):
    success: bool
    execution: Optional[ScheduleJobExecution] = None
    error: Optional[str] = None

class JobHistoryResponse(BaseModel):
    success: bool
    executions: List[ScheduleJobExecution]
    total: int


# ============================================
# SCHEMA COMPARISON MODELS
# ============================================

class SchemaComparisonRequest(BaseModel):
    source_connection_id: str
    source_catalog: Optional[str] = None
    source_schema: Optional[str] = None
    target_catalog: str
    target_schema: str

class TableComparisonRequest(BaseModel):
    source_connection_id: str
    source_catalog: Optional[str] = None
    source_schema: str
    source_table: str
    target_catalog: str
    target_schema: str
    target_table: str

class ColumnDifference(BaseModel):
    column_name: str
    difference_type: str  # missing_in_target, missing_in_source, type_mismatch, nullability_mismatch
    source_type: Optional[str] = None
    target_type: Optional[str] = None
    source_nullable: Optional[bool] = None
    target_nullable: Optional[bool] = None

class TableComparison(BaseModel):
    table_name: str
    status: str  # match, missing_in_target, missing_in_source, different
    column_differences: List[ColumnDifference] = []
    source_column_count: Optional[int] = None
    target_column_count: Optional[int] = None

class SchemaComparisonResponse(BaseModel):
    success: bool
    source_info: Dict[str, Any]
    target_info: Dict[str, Any]
    tables_only_in_source: List[str] = []
    tables_only_in_target: List[str] = []
    tables_in_both: List[TableComparison] = []
    summary: Dict[str, int]
    error: Optional[str] = None

class DataTypeMappingResponse(BaseModel):
    source_system: str
    mappings: List[Dict[str, str]]
