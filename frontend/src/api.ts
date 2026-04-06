import type {
  TranslateSqlRequest,
  TranslateSqlResponse,
  ConvertDdlRequest,
  ConvertDdlResponse,
  ConnectionConfig,
  ConnectionTestResult,
  InventoryItem,
  MigrationJob,
  Schedule,
  SchemaComparison,
  CostEstimate,
  TestQueryResult,
  Snapshot,
  MigrationHistoryItem,
  HealthResponse,
  ModelInfo,
} from './types'

const BASE_URL = '/api'

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(err.detail || `Request failed: ${res.status}`)
  }
  return res.json()
}

function post<T>(path: string, data: unknown): Promise<T> {
  return request<T>(path, { method: 'POST', body: JSON.stringify(data) })
}

function del<T>(path: string): Promise<T> {
  return request<T>(path, { method: 'DELETE' })
}

export const api = {
  // Health
  health: () => request<HealthResponse>('/health'),

  // Models
  getModels: () => request<ModelInfo[]>('/models'),

  // SQL Translation
  translateSql: (data: TranslateSqlRequest) =>
    post<TranslateSqlResponse>('/translate-sql', data),

  executeSql: (sql: string) =>
    post<{ results: Record<string, unknown>[]; row_count: number }>('/execute-sql', { sql }),

  // DDL Conversion
  convertDdl: (data: ConvertDdlRequest) =>
    post<ConvertDdlResponse>('/convert-ddl', data),

  // Connection
  testConnection: (config: ConnectionConfig) =>
    post<ConnectionTestResult>('/connection/test', config),

  extractInventory: (config: ConnectionConfig) =>
    post<InventoryItem[]>('/connection/inventory', config),

  // Migration
  startMigration: (data: { tables: string[]; source_config: ConnectionConfig; options?: Record<string, unknown> }) =>
    post<MigrationJob>('/migration/start', data),

  getMigrationStatus: (jobId: string) =>
    request<MigrationJob>(`/migration/status/${jobId}`),

  listMigrations: () => request<MigrationJob[]>('/migration/list'),

  // Schedules
  createSchedule: (data: Omit<Schedule, 'id' | 'last_run' | 'next_run'>) =>
    post<Schedule>('/schedules', data),

  listSchedules: () => request<Schedule[]>('/schedules'),

  deleteSchedule: (id: string) => del<{ success: boolean }>(`/schedules/${id}`),

  toggleSchedule: (id: string, enabled: boolean) =>
    post<Schedule>(`/schedules/${id}/toggle`, { enabled }),

  getScheduleHistory: (id: string) =>
    request<MigrationHistoryItem[]>(`/schedules/${id}/history`),

  // Schema Compare
  compareSchemas: (data: { source_table: string; target_table: string }) =>
    post<SchemaComparison>('/schema/compare', data),

  // Cost Estimator
  estimateCost: (data: { tables: string[]; storage_gb: number; daily_queries: number }) =>
    post<CostEstimate>('/cost/estimate', data),

  // Test Queries
  runTestQueries: (data: { queries: string[] }) =>
    post<TestQueryResult[]>('/test/queries', data),

  // Snapshots / Rollback
  createSnapshot: (data: { name: string; tables: string[] }) =>
    post<Snapshot>('/snapshots', data),

  listSnapshots: () => request<Snapshot[]>('/snapshots'),

  restoreSnapshot: (id: string) =>
    post<{ success: boolean; message: string }>(`/snapshots/${id}/restore`, {}),

  deleteSnapshot: (id: string) => del<{ success: boolean }>(`/snapshots/${id}`),

  validateSnapshot: (id: string) =>
    request<{ valid: boolean; issues: string[] }>(`/snapshots/${id}/validate`),

  // Migration History
  getMigrationHistory: () => request<MigrationHistoryItem[]>('/history'),

  getMigrationStats: () =>
    request<{ total: number; successful: number; failed: number; rows_migrated: number; avg_duration: number }>('/history/stats'),
}
