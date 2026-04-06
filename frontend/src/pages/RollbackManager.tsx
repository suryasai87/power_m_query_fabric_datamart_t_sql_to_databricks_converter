import { useState, useEffect } from 'react'
import {
  Box,
  Card,
  CardContent,
  Typography,
  TextField,
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
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
} from '@mui/material'
import {
  Restore as RestoreIcon,
  Add as AddIcon,
  Delete as DeleteIcon,
  Verified as ValidateIcon,
  CameraAlt as SnapshotIcon,
  CheckCircle as CheckIcon,
  Warning as WarningIcon,
} from '@mui/icons-material'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { Snapshot } from '../types'

export default function RollbackManager() {
  const [snapshots, setSnapshots] = useState<Snapshot[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [actionLoading, setActionLoading] = useState<string | null>(null)

  // Create dialog
  const [createOpen, setCreateOpen] = useState(false)
  const [snapshotName, setSnapshotName] = useState('')
  const [snapshotTables, setSnapshotTables] = useState('')
  const [creating, setCreating] = useState(false)

  // Validate dialog
  const [validateResult, setValidateResult] = useState<{ valid: boolean; issues: string[] } | null>(null)
  const [validateOpen, setValidateOpen] = useState(false)

  useEffect(() => {
    loadSnapshots()
  }, [])

  const loadSnapshots = async () => {
    try {
      const data = await api.listSnapshots()
      setSnapshots(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load snapshots')
    } finally {
      setLoading(false)
    }
  }

  const handleCreate = async () => {
    if (!snapshotName || !snapshotTables.trim()) return
    setCreating(true)
    setError('')
    try {
      const snapshot = await api.createSnapshot({
        name: snapshotName,
        tables: snapshotTables.split(',').map((t) => t.trim()).filter(Boolean),
      })
      setSnapshots((prev) => [snapshot, ...prev])
      setCreateOpen(false)
      setSnapshotName('')
      setSnapshotTables('')
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to create snapshot')
    } finally {
      setCreating(false)
    }
  }

  const handleValidate = async (id: string) => {
    setActionLoading(id)
    try {
      const result = await api.validateSnapshot(id)
      setValidateResult(result)
      setValidateOpen(true)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Validation failed')
    } finally {
      setActionLoading(null)
    }
  }

  const handleRestore = async (id: string) => {
    if (!confirm('Are you sure you want to restore this snapshot? This will overwrite current data.')) return
    setActionLoading(id)
    setError('')
    try {
      const result = await api.restoreSnapshot(id)
      if (result.success) {
        setSnapshots((prev) =>
          prev.map((s) => (s.id === id ? { ...s, status: 'active' as const } : s))
        )
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Restore failed')
    } finally {
      setActionLoading(null)
    }
  }

  const handleDelete = async (id: string) => {
    if (!confirm('Delete this snapshot? This cannot be undone.')) return
    setActionLoading(id)
    try {
      await api.deleteSnapshot(id)
      setSnapshots((prev) => prev.filter((s) => s.id !== id))
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to delete snapshot')
    } finally {
      setActionLoading(null)
    }
  }

  const statusChip = (status: Snapshot['status']) => {
    const cfg: Record<string, { color: 'success' | 'warning' | 'default' }> = {
      active: { color: 'success' },
      restoring: { color: 'warning' },
      expired: { color: 'default' },
    }
    return <Chip size="small" label={status} color={cfg[status]?.color || 'default'} />
  }

  return (
    <Box>
      {error && <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError('')}>{error}</Alert>}

      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h5">Rollback Manager</Typography>
        <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
          <Button variant="contained" startIcon={<SnapshotIcon />} onClick={() => setCreateOpen(true)}>
            Create Snapshot
          </Button>
        </motion.div>
      </Box>

      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      ) : snapshots.length === 0 ? (
        <Card>
          <CardContent sx={{ textAlign: 'center', py: 6 }}>
            <RestoreIcon sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
            <Typography color="text.secondary">
              No snapshots created yet. Create a snapshot before performing migrations to enable rollback.
            </Typography>
          </CardContent>
        </Card>
      ) : (
        <Grid container spacing={3}>
          {snapshots.map((snapshot, idx) => (
            <Grid item xs={12} md={6} key={snapshot.id}>
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: idx * 0.08 }}
              >
                <Card>
                  <CardContent>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
                      <Box>
                        <Typography variant="h6">{snapshot.name}</Typography>
                        <Typography variant="caption" color="text.secondary">
                          {new Date(snapshot.created_at).toLocaleString()}
                        </Typography>
                      </Box>
                      {statusChip(snapshot.status)}
                    </Box>

                    <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
                      <Chip size="small" label={`${snapshot.tables.length} tables`} variant="outlined" />
                      <Chip size="small" label={`${snapshot.size_mb.toFixed(1)} MB`} variant="outlined" />
                    </Box>

                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mb: 2 }}>
                      {snapshot.tables.slice(0, 5).map((t) => (
                        <Chip
                          key={t}
                          size="small"
                          label={t}
                          sx={{ fontFamily: 'monospace', fontSize: 11 }}
                        />
                      ))}
                      {snapshot.tables.length > 5 && (
                        <Chip size="small" label={`+${snapshot.tables.length - 5} more`} />
                      )}
                    </Box>

                    <Box sx={{ display: 'flex', gap: 1 }}>
                      <Button
                        size="small"
                        variant="outlined"
                        startIcon={actionLoading === snapshot.id ? <CircularProgress size={14} /> : <ValidateIcon />}
                        onClick={() => handleValidate(snapshot.id)}
                        disabled={actionLoading === snapshot.id}
                      >
                        Validate
                      </Button>
                      <Button
                        size="small"
                        variant="outlined"
                        color="warning"
                        startIcon={<RestoreIcon />}
                        onClick={() => handleRestore(snapshot.id)}
                        disabled={actionLoading === snapshot.id || snapshot.status === 'expired'}
                      >
                        Restore
                      </Button>
                      <IconButton
                        size="small"
                        color="error"
                        onClick={() => handleDelete(snapshot.id)}
                        disabled={actionLoading === snapshot.id}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Box>
                  </CardContent>
                </Card>
              </motion.div>
            </Grid>
          ))}
        </Grid>
      )}

      {/* Create Snapshot Dialog */}
      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Create Snapshot</DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5, pt: 1 }}>
            <TextField
              label="Snapshot Name"
              value={snapshotName}
              onChange={(e) => setSnapshotName(e.target.value)}
              fullWidth
              placeholder="e.g., pre-migration-2024-01"
            />
            <TextField
              label="Tables (comma-separated)"
              value={snapshotTables}
              onChange={(e) => setSnapshotTables(e.target.value)}
              fullWidth
              multiline
              rows={3}
              placeholder="customers, orders, products"
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={handleCreate}
            disabled={creating || !snapshotName || !snapshotTables.trim()}
            startIcon={creating ? <CircularProgress size={18} color="inherit" /> : <AddIcon />}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>

      {/* Validate Dialog */}
      <Dialog open={validateOpen} onClose={() => setValidateOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Snapshot Validation</DialogTitle>
        <DialogContent>
          {validateResult && (
            <Box>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                {validateResult.valid ? (
                  <CheckIcon color="success" />
                ) : (
                  <WarningIcon color="warning" />
                )}
                <Typography variant="h6">
                  {validateResult.valid ? 'Snapshot Valid' : 'Issues Found'}
                </Typography>
              </Box>
              {validateResult.issues.length > 0 && (
                <List dense>
                  {validateResult.issues.map((issue, i) => (
                    <ListItem key={i}>
                      <ListItemIcon sx={{ minWidth: 32 }}>
                        <WarningIcon fontSize="small" color="warning" />
                      </ListItemIcon>
                      <ListItemText primary={issue} />
                    </ListItem>
                  ))}
                </List>
              )}
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setValidateOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}
