import { useState, useEffect } from 'react'
import {
  Box,
  Card,
  CardContent,
  Typography,
  TextField,
  Button,
  Grid,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Alert,
  CircularProgress,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Switch,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material'
import {
  Add as AddIcon,
  Delete as DeleteIcon,
  History as HistoryIcon,
  Schedule as ScheduleIcon,
} from '@mui/icons-material'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { Schedule, MigrationHistoryItem } from '../types'

export default function Scheduler() {
  const [schedules, setSchedules] = useState<Schedule[]>([])
  const [history, setHistory] = useState<MigrationHistoryItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [dialogOpen, setDialogOpen] = useState(false)
  const [historyDialogOpen, setHistoryDialogOpen] = useState(false)
  const [selectedScheduleId, setSelectedScheduleId] = useState('')

  // New schedule form
  const [name, setName] = useState('')
  const [frequency, setFrequency] = useState('daily')
  const [cronExpression, setCronExpression] = useState('0 2 * * *')
  const [tablesInput, setTablesInput] = useState('')
  const [creating, setCreating] = useState(false)

  useEffect(() => {
    loadSchedules()
  }, [])

  const loadSchedules = async () => {
    try {
      const data = await api.listSchedules()
      setSchedules(Array.isArray(data) ? data : [])
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load schedules')
    } finally {
      setLoading(false)
    }
  }

  const handleCreate = async () => {
    if (!name || !tablesInput.trim()) return
    setCreating(true)
    setError('')
    try {
      const schedule = await api.createSchedule({
        name,
        frequency,
        cron_expression: cronExpression,
        tables: tablesInput.split(',').map((t) => t.trim()).filter(Boolean),
        enabled: true,
      })
      setSchedules((prev) => [...prev, schedule])
      setDialogOpen(false)
      setName('')
      setTablesInput('')
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to create schedule')
    } finally {
      setCreating(false)
    }
  }

  const handleToggle = async (id: string, enabled: boolean) => {
    try {
      const updated = await api.toggleSchedule(id, !enabled)
      setSchedules((prev) => prev.map((s) => (s.id === id ? updated : s)))
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to toggle schedule')
    }
  }

  const handleDelete = async (id: string) => {
    try {
      await api.deleteSchedule(id)
      setSchedules((prev) => prev.filter((s) => s.id !== id))
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to delete schedule')
    }
  }

  const handleViewHistory = async (id: string) => {
    setSelectedScheduleId(id)
    setHistoryDialogOpen(true)
    try {
      const data = await api.getScheduleHistory(id)
      setHistory(Array.isArray(data) ? data : [])
    } catch {
      setHistory([])
    }
  }

  const frequencyToCron: Record<string, string> = {
    hourly: '0 * * * *',
    daily: '0 2 * * *',
    weekly: '0 2 * * 1',
    monthly: '0 2 1 * *',
  }

  const handleFrequencyChange = (freq: string) => {
    setFrequency(freq)
    setCronExpression(frequencyToCron[freq] || '0 2 * * *')
  }

  return (
    <Box>
      {error && <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError('')}>{error}</Alert>}

      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h5">Migration Schedules</Typography>
        <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setDialogOpen(true)}>
            Create Schedule
          </Button>
        </motion.div>
      </Box>

      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      ) : schedules.length === 0 ? (
        <Card>
          <CardContent sx={{ textAlign: 'center', py: 6 }}>
            <ScheduleIcon sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
            <Typography color="text.secondary">
              No schedules configured. Click "Create Schedule" to set up automated migrations.
            </Typography>
          </CardContent>
        </Card>
      ) : (
        <Card>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Name</TableCell>
                  <TableCell>Frequency</TableCell>
                  <TableCell>Cron</TableCell>
                  <TableCell>Tables</TableCell>
                  <TableCell>Last Run</TableCell>
                  <TableCell>Next Run</TableCell>
                  <TableCell>Enabled</TableCell>
                  <TableCell align="right">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {schedules.map((schedule) => (
                  <TableRow key={schedule.id} hover>
                    <TableCell>
                      <Typography variant="body2" fontWeight={600}>{schedule.name}</Typography>
                    </TableCell>
                    <TableCell>
                      <Chip size="small" label={schedule.frequency} variant="outlined" />
                    </TableCell>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: 13 }}>
                      {schedule.cron_expression}
                    </TableCell>
                    <TableCell>
                      <Chip size="small" label={`${schedule.tables.length} tables`} />
                    </TableCell>
                    <TableCell>
                      {schedule.last_run
                        ? new Date(schedule.last_run).toLocaleString()
                        : '--'}
                    </TableCell>
                    <TableCell>
                      {schedule.next_run
                        ? new Date(schedule.next_run).toLocaleString()
                        : '--'}
                    </TableCell>
                    <TableCell>
                      <Switch
                        checked={schedule.enabled}
                        onChange={() => handleToggle(schedule.id, schedule.enabled)}
                        size="small"
                      />
                    </TableCell>
                    <TableCell align="right">
                      <IconButton size="small" onClick={() => handleViewHistory(schedule.id)}>
                        <HistoryIcon fontSize="small" />
                      </IconButton>
                      <IconButton size="small" color="error" onClick={() => handleDelete(schedule.id)}>
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Card>
      )}

      {/* Create Schedule Dialog */}
      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Create Migration Schedule</DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5, pt: 1 }}>
            <TextField
              label="Schedule Name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              fullWidth
              placeholder="e.g., Nightly Customer Sync"
            />

            <FormControl fullWidth>
              <InputLabel>Frequency</InputLabel>
              <Select value={frequency} label="Frequency" onChange={(e) => handleFrequencyChange(e.target.value)}>
                <MenuItem value="hourly">Hourly</MenuItem>
                <MenuItem value="daily">Daily</MenuItem>
                <MenuItem value="weekly">Weekly</MenuItem>
                <MenuItem value="monthly">Monthly</MenuItem>
                <MenuItem value="custom">Custom Cron</MenuItem>
              </Select>
            </FormControl>

            <TextField
              label="Cron Expression"
              value={cronExpression}
              onChange={(e) => setCronExpression(e.target.value)}
              fullWidth
              helperText="Standard cron format: minute hour day month weekday"
              sx={{ '& .MuiInputBase-root': { fontFamily: 'monospace' } }}
            />

            <TextField
              label="Tables (comma-separated)"
              value={tablesInput}
              onChange={(e) => setTablesInput(e.target.value)}
              fullWidth
              multiline
              rows={3}
              placeholder="dbo.customers, dbo.orders, dbo.products"
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={handleCreate}
            disabled={creating || !name || !tablesInput.trim()}
            startIcon={creating ? <CircularProgress size={18} color="inherit" /> : <AddIcon />}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>

      {/* History Dialog */}
      <Dialog open={historyDialogOpen} onClose={() => setHistoryDialogOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>Execution History</DialogTitle>
        <DialogContent>
          {history.length === 0 ? (
            <Typography color="text.secondary" sx={{ py: 3, textAlign: 'center' }}>
              No execution history for this schedule.
            </Typography>
          ) : (
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Started</TableCell>
                    <TableCell>Duration</TableCell>
                    <TableCell>Tables</TableCell>
                    <TableCell>Rows</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {history.map((h) => (
                    <TableRow key={h.id}>
                      <TableCell>{new Date(h.started_at).toLocaleString()}</TableCell>
                      <TableCell>{h.duration_seconds}s</TableCell>
                      <TableCell>{h.tables_count}</TableCell>
                      <TableCell>{h.rows_migrated.toLocaleString()}</TableCell>
                      <TableCell>
                        <Chip
                          size="small"
                          label={h.status}
                          color={h.status === 'completed' ? 'success' : h.status === 'failed' ? 'error' : 'warning'}
                        />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setHistoryDialogOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}
