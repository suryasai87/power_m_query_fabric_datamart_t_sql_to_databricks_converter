import { useState, useEffect, useRef } from 'react'
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
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
  Checkbox,
  LinearProgress,
  FormControlLabel,
  Switch,
  TextField,
} from '@mui/material'
import {
  CloudSync as MigrateIcon,
  PlayArrow as StartIcon,
  Stop as StopIcon,
  CheckCircle as DoneIcon,
  Error as ErrorIcon,
} from '@mui/icons-material'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { MigrationJob, MigrationProgress } from '../types'

interface TableEntry {
  name: string
  schema: string
  row_count: number
  selected: boolean
}

export default function BulkMigration() {
  const [tables, setTables] = useState<TableEntry[]>([
    { name: 'customers', schema: 'dbo', row_count: 150000, selected: true },
    { name: 'orders', schema: 'dbo', row_count: 1200000, selected: true },
    { name: 'products', schema: 'dbo', row_count: 5000, selected: false },
    { name: 'order_items', schema: 'dbo', row_count: 3500000, selected: true },
    { name: 'inventory', schema: 'dbo', row_count: 25000, selected: false },
    { name: 'suppliers', schema: 'dbo', row_count: 800, selected: false },
    { name: 'shipments', schema: 'dbo', row_count: 950000, selected: false },
    { name: 'payments', schema: 'dbo', row_count: 1100000, selected: false },
  ])
  const [job, setJob] = useState<MigrationJob | null>(null)
  const [progress, setProgress] = useState<MigrationProgress[]>([])
  const [parallelism, setParallelism] = useState(4)
  const [incrementalMode, setIncrementalMode] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const eventSourceRef = useRef<EventSource | null>(null)

  const selectedTables = tables.filter((t) => t.selected)
  const allSelected = tables.every((t) => t.selected)

  const toggleAll = () => {
    const newVal = !allSelected
    setTables(tables.map((t) => ({ ...t, selected: newVal })))
  }

  const toggleTable = (name: string) => {
    setTables(tables.map((t) => (t.name === name ? { ...t, selected: !t.selected } : t)))
  }

  const handleStartMigration = async () => {
    if (selectedTables.length === 0) return
    setLoading(true)
    setError('')
    setProgress([])
    try {
      const result = await api.startMigration({
        tables: selectedTables.map((t) => `${t.schema}.${t.name}`),
        source_config: {
          source_type: 'sqlserver',
          host: 'configured-host',
          port: 1433,
          database: 'configured-db',
          username: 'user',
          password: 'pass',
        },
        options: { parallelism, incremental: incrementalMode },
      })
      setJob(result)

      // SSE for progress updates
      const es = new EventSource(`/api/migration/progress/${result.id}`)
      eventSourceRef.current = es
      es.onmessage = (event) => {
        const data: MigrationProgress = JSON.parse(event.data)
        setProgress((prev) => {
          const idx = prev.findIndex((p) => p.table === data.table)
          if (idx >= 0) {
            const updated = [...prev]
            updated[idx] = data
            return updated
          }
          return [...prev, data]
        })
        if (data.status === 'completed' || data.status === 'failed') {
          // Check if all tables done
          setJob((prev) =>
            prev ? { ...prev, progress: data.progress } : prev
          )
        }
      }
      es.onerror = () => {
        es.close()
        setJob((prev) => (prev ? { ...prev, status: 'completed' } : prev))
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to start migration')
    } finally {
      setLoading(false)
    }
  }

  const handleStop = () => {
    eventSourceRef.current?.close()
    setJob((prev) => (prev ? { ...prev, status: 'failed' } : prev))
  }

  useEffect(() => {
    return () => {
      eventSourceRef.current?.close()
    }
  }, [])

  const overallProgress = progress.length > 0
    ? Math.round(progress.reduce((sum, p) => sum + p.progress, 0) / Math.max(progress.length, 1))
    : 0

  return (
    <Box>
      <Grid container spacing={3}>
        {/* Table Selector */}
        <Grid size={{ xs: 12, md: 7 }}>
          <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
                  <Typography variant="h6">
                    Select Tables
                    <Chip size="small" label={`${selectedTables.length} selected`} sx={{ ml: 1 }} />
                  </Typography>
                </Box>

                <TableContainer sx={{ maxHeight: 480 }}>
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        <TableCell padding="checkbox">
                          <Checkbox checked={allSelected} onChange={toggleAll} />
                        </TableCell>
                        <TableCell>Table</TableCell>
                        <TableCell>Schema</TableCell>
                        <TableCell align="right">Rows</TableCell>
                        <TableCell>Status</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {tables.map((table) => {
                        const p = progress.find((pr) => pr.table === `${table.schema}.${table.name}`)
                        return (
                          <TableRow key={table.name} hover>
                            <TableCell padding="checkbox">
                              <Checkbox
                                checked={table.selected}
                                onChange={() => toggleTable(table.name)}
                                disabled={!!job && job.status === 'running'}
                              />
                            </TableCell>
                            <TableCell sx={{ fontFamily: 'monospace', fontSize: 13 }}>
                              {table.name}
                            </TableCell>
                            <TableCell>{table.schema}</TableCell>
                            <TableCell align="right">{table.row_count.toLocaleString()}</TableCell>
                            <TableCell>
                              {p ? (
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, minWidth: 140 }}>
                                  {p.status === 'completed' ? (
                                    <DoneIcon fontSize="small" color="success" />
                                  ) : p.status === 'failed' ? (
                                    <ErrorIcon fontSize="small" color="error" />
                                  ) : (
                                    <CircularProgress size={16} />
                                  )}
                                  <Box sx={{ flexGrow: 1 }}>
                                    <LinearProgress
                                      variant="determinate"
                                      value={p.progress}
                                      sx={{ height: 6, borderRadius: 3 }}
                                    />
                                  </Box>
                                  <Typography variant="caption">{p.progress}%</Typography>
                                </Box>
                              ) : table.selected && job ? (
                                <Chip size="small" label="Queued" variant="outlined" />
                              ) : (
                                <Typography variant="caption" color="text.secondary">--</Typography>
                              )}
                            </TableCell>
                          </TableRow>
                        )
                      })}
                    </TableBody>
                  </Table>
                </TableContainer>
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Migration Options & Progress */}
        <Grid size={{ xs: 12, md: 5 }}>
          <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.1 }}>
            <Card sx={{ mb: 3 }}>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Migration Options
                </Typography>

                {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <TextField
                    size="small"
                    label="Parallelism"
                    type="number"
                    value={parallelism}
                    onChange={(e) => setParallelism(parseInt(e.target.value) || 1)}
                    slotProps={{ htmlInput: { min: 1, max: 16 } }}
                    helperText="Number of tables migrated simultaneously"
                    fullWidth
                  />

                  <FormControlLabel
                    control={
                      <Switch
                        checked={incrementalMode}
                        onChange={(e) => setIncrementalMode(e.target.checked)}
                      />
                    }
                    label="Incremental mode (sync only changed rows)"
                  />

                  <Box sx={{ display: 'flex', gap: 1 }}>
                    <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                      <Button
                        variant="contained"
                        startIcon={loading ? <CircularProgress size={18} color="inherit" /> : <StartIcon />}
                        onClick={handleStartMigration}
                        disabled={loading || selectedTables.length === 0 || (!!job && job.status === 'running')}
                        fullWidth
                      >
                        {loading ? 'Starting...' : `Migrate ${selectedTables.length} Tables`}
                      </Button>
                    </motion.div>

                    {job?.status === 'running' && (
                      <Button
                        variant="outlined"
                        color="error"
                        startIcon={<StopIcon />}
                        onClick={handleStop}
                      >
                        Stop
                      </Button>
                    )}
                  </Box>
                </Box>
              </CardContent>
            </Card>

            {/* Overall Progress */}
            {job && (
              <motion.div initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }}>
                <Card>
                  <CardContent>
                    <Typography variant="h6" gutterBottom>
                      Migration Progress
                    </Typography>

                    <Box sx={{ mb: 2 }}>
                      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
                        <Typography variant="body2" color="text.secondary">Overall Progress</Typography>
                        <Typography variant="body2">{overallProgress}%</Typography>
                      </Box>
                      <LinearProgress
                        variant="determinate"
                        value={overallProgress}
                        sx={{ height: 10, borderRadius: 5 }}
                      />
                    </Box>

                    <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Typography variant="body2" color="text.secondary">Status</Typography>
                      <Chip
                        size="small"
                        label={job.status}
                        color={
                          job.status === 'completed' ? 'success'
                            : job.status === 'failed' ? 'error'
                              : job.status === 'running' ? 'info'
                                : 'default'
                        }
                      />
                    </Box>

                    {progress.length > 0 && (
                      <Box sx={{ mt: 2 }}>
                        <Typography variant="subtitle2" gutterBottom>
                          Table Details
                        </Typography>
                        {progress.map((p) => (
                          <Box key={p.table} sx={{ mb: 1 }}>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                              <Typography variant="caption" sx={{ fontFamily: 'monospace' }}>
                                {p.table}
                              </Typography>
                              <Typography variant="caption">
                                {p.rows_migrated.toLocaleString()} / {p.total_rows.toLocaleString()}
                              </Typography>
                            </Box>
                            <LinearProgress
                              variant="determinate"
                              value={p.progress}
                              color={p.status === 'failed' ? 'error' : p.status === 'completed' ? 'success' : 'primary'}
                              sx={{ height: 4, borderRadius: 2 }}
                            />
                          </Box>
                        ))}
                      </Box>
                    )}
                  </CardContent>
                </Card>
              </motion.div>
            )}
          </motion.div>
        </Grid>
      </Grid>
    </Box>
  )
}
