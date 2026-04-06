export interface TranslateSqlRequest {
  source_sql: string
  source_dialect?: string
  model?: string
}

export interface TranslateSqlResponse {
  translated_sql: string
  source_dialect: string
  model_used: string
  warnings?: string[]
}

export interface ConvertDdlRequest {
  ddl: string
  target_catalog?: string
  target_schema?: string
}

export interface ConvertDdlResponse {
  converted_ddl: string
  tables_created: string[]
  warnings?: string[]
}

export interface ConnectionConfig {
  source_type: string
  host: string
  port: number
  database: string
  username: string
  password: string
  schema?: string
}

export interface ConnectionTestResult {
  success: boolean
  message: string
  server_version?: string
}

export interface InventoryItem {
  name: string
  type: string
  schema: string
  row_count?: number
  size_mb?: number
}

export interface MigrationJob {
  id: string
  source_type: string
  tables: string[]
  status: 'pending' | 'running' | 'completed' | 'failed'
  progress: number
  started_at?: string
  completed_at?: string
  error?: string
}

export interface MigrationProgress {
  job_id: string
  table: string
  status: string
  progress: number
  rows_migrated: number
  total_rows: number
}

export interface Schedule {
  id: string
  name: string
  frequency: string
  cron_expression: string
  tables: string[]
  enabled: boolean
  last_run?: string
  next_run?: string
}

export interface SchemaComparison {
  table_name: string
  source_columns: ColumnDef[]
  target_columns: ColumnDef[]
  differences: ColumnDiff[]
}

export interface ColumnDef {
  name: string
  type: string
  nullable: boolean
}

export interface ColumnDiff {
  column_name: string
  source_type: string
  target_type: string
  diff_type: 'added' | 'removed' | 'modified' | 'type_change'
}

export interface CostEstimate {
  storage_cost: number
  compute_cost: number
  migration_cost: number
  total_monthly: number
  currency: string
}

export interface TestQueryResult {
  query: string
  source_result: string
  target_result: string
  match: boolean
  execution_time_ms: number
}

export interface Snapshot {
  id: string
  name: string
  created_at: string
  tables: string[]
  size_mb: number
  status: 'active' | 'restoring' | 'expired'
}

export interface MigrationHistoryItem {
  id: string
  job_name: string
  source_type: string
  tables_count: number
  status: 'completed' | 'failed' | 'partial'
  started_at: string
  completed_at: string
  rows_migrated: number
  duration_seconds: number
}

export interface HealthResponse {
  status: string
  version: string
  uptime: number
}

export interface ModelInfo {
  id: string
  name: string
  provider: string
}
