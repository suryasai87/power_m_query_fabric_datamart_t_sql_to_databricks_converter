import { useState, useEffect } from 'react'
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  Alert,
  CircularProgress,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  InputAdornment,
} from '@mui/material'
import {
  Search as SearchIcon,
  CheckCircle as SuccessIcon,
  Error as ErrorIcon,
  Warning as WarningIcon,
} from '@mui/icons-material'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area,
} from 'recharts'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { MigrationHistoryItem } from '../types'

export default function MigrationHistory() {
  const [history, setHistory] = useState<MigrationHistoryItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [search, setSearch] = useState('')

  useEffect(() => {
    async function load() {
      try {
        const data = await api.getMigrationHistory()
        setHistory(data)
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to load history')
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  const filtered = history.filter(
    (h) =>
      h.job_name.toLowerCase().includes(search.toLowerCase()) ||
      h.source_type.toLowerCase().includes(search.toLowerCase())
  )

  const statusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <SuccessIcon fontSize="small" color="success" />
      case 'failed':
        return <ErrorIcon fontSize="small" color="error" />
      default:
        return <WarningIcon fontSize="small" color="warning" />
    }
  }

  const statusColor = (status: string): 'success' | 'error' | 'warning' => {
    switch (status) {
      case 'completed':
        return 'success'
      case 'failed':
        return 'error'
      default:
        return 'warning'
    }
  }

  // Build trend data from history
  const trendData = (() => {
    const byDate: Record<string, { date: string; successful: number; failed: number; rows: number }> = {}
    history.forEach((h) => {
      const date = new Date(h.started_at).toLocaleDateString()
      if (!byDate[date]) byDate[date] = { date, successful: 0, failed: 0, rows: 0 }
      if (h.status === 'completed') byDate[date].successful++
      else if (h.status === 'failed') byDate[date].failed++
      byDate[date].rows += h.rows_migrated
    })
    return Object.values(byDate).slice(-14)
  })()

  // Summary stats
  const totalJobs = history.length
  const successJobs = history.filter((h) => h.status === 'completed').length
  const failedJobs = history.filter((h) => h.status === 'failed').length
  const totalRows = history.reduce((sum, h) => sum + h.rows_migrated, 0)

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
        <CircularProgress />
      </Box>
    )
  }

  return (
    <Box>
      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      <Grid container spacing={3}>
        {/* Summary Cards */}
        {[
          { label: 'Total Jobs', value: totalJobs, color: '#2196F3' },
          { label: 'Successful', value: successJobs, color: '#00A972' },
          { label: 'Failed', value: failedJobs, color: '#FF3621' },
          { label: 'Total Rows', value: totalRows.toLocaleString(), color: '#FFB020' },
        ].map((card, i) => (
          <Grid size={{ xs: 6, md: 3 }} key={card.label}>
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.06 }}
            >
              <Card>
                <CardContent sx={{ textAlign: 'center', py: 2 }}>
                  <Typography variant="caption" color="text.secondary">{card.label}</Typography>
                  <Typography variant="h5" sx={{ color: card.color }}>{card.value}</Typography>
                </CardContent>
              </Card>
            </motion.div>
          </Grid>
        ))}

        {/* Migration Trends Line Chart */}
        <Grid size={{ xs: 12, md: 7 }}>
          <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.25 }}>
            <Card sx={{ height: 360 }}>
              <CardContent>
                <Typography variant="h6" gutterBottom>Migration Trends</Typography>
                {trendData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={280}>
                    <LineChart data={trendData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                      <XAxis dataKey="date" stroke="#B0B0B8" fontSize={12} />
                      <YAxis stroke="#B0B0B8" />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#2A2A30',
                          border: '1px solid rgba(255,255,255,0.1)',
                          borderRadius: 8,
                          color: '#F5F5F5',
                        }}
                      />
                      <Line type="monotone" dataKey="successful" stroke="#00A972" strokeWidth={2} dot={{ r: 4 }} />
                      <Line type="monotone" dataKey="failed" stroke="#FF3621" strokeWidth={2} dot={{ r: 4 }} />
                    </LineChart>
                  </ResponsiveContainer>
                ) : (
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 280 }}>
                    <Typography color="text.secondary">No trend data available yet.</Typography>
                  </Box>
                )}
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Rows Migrated Area Chart */}
        <Grid size={{ xs: 12, md: 5 }}>
          <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.3 }}>
            <Card sx={{ height: 360 }}>
              <CardContent>
                <Typography variant="h6" gutterBottom>Rows Migrated</Typography>
                {trendData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={280}>
                    <AreaChart data={trendData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                      <XAxis dataKey="date" stroke="#B0B0B8" fontSize={12} />
                      <YAxis stroke="#B0B0B8" tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#2A2A30',
                          border: '1px solid rgba(255,255,255,0.1)',
                          borderRadius: 8,
                          color: '#F5F5F5',
                        }}
                        formatter={(value: number) => [value.toLocaleString(), 'Rows']}
                      />
                      <Area
                        type="monotone"
                        dataKey="rows"
                        stroke="#FFB020"
                        fill="rgba(255, 176, 32, 0.15)"
                        strokeWidth={2}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                ) : (
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 280 }}>
                    <Typography color="text.secondary">No data available yet.</Typography>
                  </Box>
                )}
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* History Table */}
        <Grid size={12}>
          <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.35 }}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
                  <Typography variant="h6">Job History</Typography>
                  <TextField
                    size="small"
                    placeholder="Search jobs..."
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    slotProps={{
                      input: {
                        startAdornment: (
                          <InputAdornment position="start">
                            <SearchIcon fontSize="small" />
                          </InputAdornment>
                        ),
                      },
                    }}
                    sx={{ width: 260 }}
                  />
                </Box>

                {filtered.length === 0 ? (
                  <Typography color="text.secondary" sx={{ py: 4, textAlign: 'center' }}>
                    {search ? 'No matching jobs found.' : 'No migration history yet.'}
                  </Typography>
                ) : (
                  <TableContainer sx={{ maxHeight: 480 }}>
                    <Table size="small" stickyHeader>
                      <TableHead>
                        <TableRow>
                          <TableCell>Status</TableCell>
                          <TableCell>Job Name</TableCell>
                          <TableCell>Source</TableCell>
                          <TableCell align="right">Tables</TableCell>
                          <TableCell align="right">Rows Migrated</TableCell>
                          <TableCell>Started</TableCell>
                          <TableCell align="right">Duration</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {filtered.map((job) => (
                          <TableRow key={job.id} hover>
                            <TableCell>
                              <Chip
                                size="small"
                                icon={statusIcon(job.status)}
                                label={job.status}
                                color={statusColor(job.status)}
                                variant="outlined"
                              />
                            </TableCell>
                            <TableCell>
                              <Typography variant="body2" fontWeight={600}>{job.job_name}</Typography>
                            </TableCell>
                            <TableCell>{job.source_type}</TableCell>
                            <TableCell align="right">{job.tables_count}</TableCell>
                            <TableCell align="right">{job.rows_migrated.toLocaleString()}</TableCell>
                            <TableCell>{new Date(job.started_at).toLocaleString()}</TableCell>
                            <TableCell align="right">
                              {job.duration_seconds < 60
                                ? `${job.duration_seconds}s`
                                : `${Math.floor(job.duration_seconds / 60)}m ${job.duration_seconds % 60}s`}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                )}
              </CardContent>
            </Card>
          </motion.div>
        </Grid>
      </Grid>
    </Box>
  )
}
