import { useState } from 'react'
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
} from '@mui/material'
import {
  CompareArrows as CompareIcon,
  Add as AddIcon,
  Remove as RemoveIcon,
  Edit as EditIcon,
  SwapHoriz as TypeChangeIcon,
} from '@mui/icons-material'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { SchemaComparison, ColumnDiff } from '../types'

const diffConfig: Record<string, { label: string; color: 'success' | 'error' | 'warning' | 'info'; icon: React.ReactElement }> = {
  added: { label: 'Added', color: 'success', icon: <AddIcon fontSize="small" /> },
  removed: { label: 'Removed', color: 'error', icon: <RemoveIcon fontSize="small" /> },
  modified: { label: 'Modified', color: 'warning', icon: <EditIcon fontSize="small" /> },
  type_change: { label: 'Type Change', color: 'info', icon: <TypeChangeIcon fontSize="small" /> },
}

export default function SchemaCompare() {
  const [sourceTable, setSourceTable] = useState('')
  const [targetTable, setTargetTable] = useState('')
  const [comparison, setComparison] = useState<SchemaComparison | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const handleCompare = async () => {
    if (!sourceTable || !targetTable) return
    setLoading(true)
    setError('')
    setComparison(null)
    try {
      const result = await api.compareSchemas({
        source_table: sourceTable,
        target_table: targetTable,
      })
      setComparison(result)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Schema comparison failed')
    } finally {
      setLoading(false)
    }
  }

  const renderDiffChip = (diff: ColumnDiff) => {
    const cfg = diffConfig[diff.diff_type] || diffConfig.modified
    return <Chip size="small" label={cfg.label} color={cfg.color} icon={cfg.icon} />
  }

  return (
    <Box>
      <Grid container spacing={3}>
        {/* Comparison Form */}
        <Grid size={12}>
          <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Schema Comparison
                </Typography>
                <Box sx={{ display: 'flex', gap: 2, alignItems: 'flex-end' }}>
                  <TextField
                    label="Source Table"
                    value={sourceTable}
                    onChange={(e) => setSourceTable(e.target.value)}
                    placeholder="dbo.customers"
                    sx={{ flex: 1, '& .MuiInputBase-root': { fontFamily: 'monospace' } }}
                  />
                  <CompareIcon sx={{ color: 'text.secondary', mb: 1 }} />
                  <TextField
                    label="Target Table (Databricks)"
                    value={targetTable}
                    onChange={(e) => setTargetTable(e.target.value)}
                    placeholder="main.default.customers"
                    sx={{ flex: 1, '& .MuiInputBase-root': { fontFamily: 'monospace' } }}
                  />
                  <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                    <Button
                      variant="contained"
                      startIcon={loading ? <CircularProgress size={18} color="inherit" /> : <CompareIcon />}
                      onClick={handleCompare}
                      disabled={loading || !sourceTable || !targetTable}
                      sx={{ mb: 0.5 }}
                    >
                      Compare
                    </Button>
                  </motion.div>
                </Box>
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {error && (
          <Grid size={12}>
            <Alert severity="error">{error}</Alert>
          </Grid>
        )}

        {comparison && (
          <>
            {/* Diff Summary */}
            <Grid size={12}>
              <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}>
                <Card>
                  <CardContent>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 2 }}>
                      <Typography variant="h6">
                        Column Differences
                      </Typography>
                      {comparison.differences.length === 0 ? (
                        <Chip label="Schemas Match" color="success" />
                      ) : (
                        <Chip label={`${comparison.differences.length} differences`} color="warning" />
                      )}
                    </Box>

                    {comparison.differences.length > 0 && (
                      <TableContainer>
                        <Table size="small">
                          <TableHead>
                            <TableRow>
                              <TableCell>Column</TableCell>
                              <TableCell>Source Type</TableCell>
                              <TableCell>Target Type</TableCell>
                              <TableCell>Difference</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {comparison.differences.map((diff) => (
                              <TableRow
                                key={diff.column_name}
                                sx={{
                                  bgcolor:
                                    diff.diff_type === 'added' ? 'rgba(0, 169, 114, 0.06)'
                                      : diff.diff_type === 'removed' ? 'rgba(255, 54, 33, 0.06)'
                                        : 'transparent',
                                }}
                              >
                                <TableCell sx={{ fontFamily: 'monospace', fontWeight: 600 }}>
                                  {diff.column_name}
                                </TableCell>
                                <TableCell sx={{ fontFamily: 'monospace', fontSize: 13 }}>
                                  {diff.source_type || '--'}
                                </TableCell>
                                <TableCell sx={{ fontFamily: 'monospace', fontSize: 13 }}>
                                  {diff.target_type || '--'}
                                </TableCell>
                                <TableCell>{renderDiffChip(diff)}</TableCell>
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

            {/* Side-by-Side Schema */}
            <Grid size={{ xs: 12, md: 6 }}>
              <motion.div initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: 0.15 }}>
                <Card>
                  <CardContent>
                    <Typography variant="h6" gutterBottom>
                      Source Schema
                      <Chip size="small" label={`${comparison.source_columns.length} columns`} sx={{ ml: 1 }} />
                    </Typography>
                    <TableContainer>
                      <Table size="small">
                        <TableHead>
                          <TableRow>
                            <TableCell>Column</TableCell>
                            <TableCell>Type</TableCell>
                            <TableCell>Nullable</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {comparison.source_columns.map((col) => {
                            const hasDiff = comparison.differences.some((d) => d.column_name === col.name)
                            return (
                              <TableRow key={col.name} sx={{ bgcolor: hasDiff ? 'rgba(255, 176, 32, 0.06)' : 'transparent' }}>
                                <TableCell sx={{ fontFamily: 'monospace', fontSize: 13 }}>{col.name}</TableCell>
                                <TableCell sx={{ fontFamily: 'monospace', fontSize: 13 }}>{col.type}</TableCell>
                                <TableCell>
                                  <Chip size="small" label={col.nullable ? 'YES' : 'NO'} variant="outlined" />
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

            <Grid size={{ xs: 12, md: 6 }}>
              <motion.div initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: 0.15 }}>
                <Card>
                  <CardContent>
                    <Typography variant="h6" gutterBottom>
                      Target Schema (Databricks)
                      <Chip size="small" label={`${comparison.target_columns.length} columns`} sx={{ ml: 1 }} />
                    </Typography>
                    <TableContainer>
                      <Table size="small">
                        <TableHead>
                          <TableRow>
                            <TableCell>Column</TableCell>
                            <TableCell>Type</TableCell>
                            <TableCell>Nullable</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {comparison.target_columns.map((col) => {
                            const hasDiff = comparison.differences.some((d) => d.column_name === col.name)
                            return (
                              <TableRow key={col.name} sx={{ bgcolor: hasDiff ? 'rgba(255, 176, 32, 0.06)' : 'transparent' }}>
                                <TableCell sx={{ fontFamily: 'monospace', fontSize: 13 }}>{col.name}</TableCell>
                                <TableCell sx={{ fontFamily: 'monospace', fontSize: 13 }}>{col.type}</TableCell>
                                <TableCell>
                                  <Chip size="small" label={col.nullable ? 'YES' : 'NO'} variant="outlined" />
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
          </>
        )}

        {!comparison && !error && (
          <Grid size={12}>
            <Card sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', py: 8 }}>
              <Box sx={{ textAlign: 'center' }}>
                <CompareIcon sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
                <Typography color="text.secondary">
                  Enter source and target table names to compare their schemas side-by-side.
                </Typography>
              </Box>
            </Card>
          </Grid>
        )}
      </Grid>
    </Box>
  )
}
