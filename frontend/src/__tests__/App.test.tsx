import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import App from '../App'

describe('App', () => {
  it('renders the app title', () => {
    render(<App />)
    expect(screen.getByText(/DW Migration Assistant/i)).toBeInTheDocument()
  })

  it('renders the subtitle', () => {
    render(<App />)
    expect(screen.getByText(/Power BI.*Databricks Converter/i)).toBeInTheDocument()
  })

  it('renders all 11 tab labels', () => {
    render(<App />)
    const tabLabels = [
      'Dashboard',
      'SQL Translator',
      'Convert DDL',
      'Connect & Migrate',
      'Bulk Migration',
      'Scheduler',
      'Schema Compare',
      'Cost Estimator',
      'Test Queries',
      'Rollback',
      'History',
    ]
    tabLabels.forEach((label) => {
      expect(screen.getByText(label)).toBeInTheDocument()
    })
  })

  it('shows Databricks chip', () => {
    render(<App />)
    expect(screen.getByText('Databricks')).toBeInTheDocument()
  })
})
