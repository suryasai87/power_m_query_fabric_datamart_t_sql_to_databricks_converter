import { describe, it, expect } from 'vitest'
import { api } from '../api'

describe('API client', () => {
  it('exports an api object', () => {
    expect(api).toBeDefined()
    expect(typeof api).toBe('object')
  })

  it('has health endpoint', () => {
    expect(typeof api.health).toBe('function')
  })

  it('has translateSql endpoint', () => {
    expect(typeof api.translateSql).toBe('function')
  })

  it('has convertDdl endpoint', () => {
    expect(typeof api.convertDdl).toBe('function')
  })

  it('has testConnection endpoint', () => {
    expect(typeof api.testConnection).toBe('function')
  })

  it('has extractInventory endpoint', () => {
    expect(typeof api.extractInventory).toBe('function')
  })

  it('has startMigration endpoint', () => {
    expect(typeof api.startMigration).toBe('function')
  })

  it('has listSchedules endpoint', () => {
    expect(typeof api.listSchedules).toBe('function')
  })

  it('has compareSchemas endpoint', () => {
    expect(typeof api.compareSchemas).toBe('function')
  })

  it('has estimateCost endpoint', () => {
    expect(typeof api.estimateCost).toBe('function')
  })

  it('has runTestQueries endpoint', () => {
    expect(typeof api.runTestQueries).toBe('function')
  })

  it('has createSnapshot endpoint', () => {
    expect(typeof api.createSnapshot).toBe('function')
  })

  it('has getMigrationHistory endpoint', () => {
    expect(typeof api.getMigrationHistory).toBe('function')
  })

  it('has getModels endpoint', () => {
    expect(typeof api.getModels).toBe('function')
  })

  it('has executeSql endpoint', () => {
    expect(typeof api.executeSql).toBe('function')
  })
})
