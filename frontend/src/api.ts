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

function put<T>(path: string, data: unknown): Promise<T> {
  return request<T>(path, { method: 'PUT', body: JSON.stringify(data) })
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
    post<ConnectionTestResult>('/connect/test', config),

  extractInventory: (config: ConnectionConfig) =>
    post<InventoryItem[]>('/connect/extract-inventory', config),

  // Migration
  startMigration: (data: { tables: string[]; source_config: ConnectionConfig; options?: Record<string, unknown> }) =>
    post<MigrationJob>('/migrate/start', data),

  getMigrationStatus: (jobId: string) =>
    request<MigrationJob>(`/migrate/progress/${jobId}`),

  listMigrations: () => request<MigrationJob[]>('/migrate/jobs'),

  // Schedules
  createSchedule: (data: Omit<Schedule, 'id' | 'last_run' | 'next_run'>) =>
    post<Schedule>('/schedule/create', data),

  listSchedules: () => request<Schedule[]>('/schedule/list'),

  deleteSchedule: (id: string) => del<{ success: boolean }>(`/schedule/${id}`),

  toggleSchedule: (id: string, enabled: boolean) =>
    put<Schedule>(`/schedule/${id}`, { enabled }),

  getScheduleHistory: (id: string) =>
    request<MigrationHistoryItem[]>('/schedule/executions/history'),

  // Schema Compare
  compareSchemas: (data: { source_table: string; target_table: string }) =>
    post<SchemaComparison>('/compare/schemas', data),

  // Cost Estimator
  estimateCost: (data: { tables: string[]; storage_gb: number; daily_queries: number }) =>
    post<CostEstimate>('/estimate/migration', data),

  // Test Queries
  runTestQueries: (data: { queries: string[] }) =>
    post<TestQueryResult[]>('/test/batch', data),

  // Snapshots / Rollback
  createSnapshot: (data: { name: string; tables: string[] }) =>
    post<Snapshot>('/rollback/snapshot', data),

  listSnapshots: () => request<Snapshot[]>('/rollback/snapshots'),

  restoreSnapshot: (id: string) =>
    post<{ success: boolean; message: string }>(`/rollback/restore/${id}`, {}),

  deleteSnapshot: (id: string) => del<{ success: boolean }>(`/rollback/snapshot/${id}`),

  validateSnapshot: (id: string) =>
    request<{ valid: boolean; issues: string[] }>('/rollback/validate'),

  // Migration History
  getMigrationHistory: () => request<MigrationHistoryItem[]>('/migrate/history'),
}
