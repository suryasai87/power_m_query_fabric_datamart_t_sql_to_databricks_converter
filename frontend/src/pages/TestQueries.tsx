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
  IconButton,
} from '@mui/material'
import {
  PlaylistAddCheck as TestIcon,
  Add as AddIcon,
  Delete as DeleteIcon,
  CheckCircle as PassIcon,
  Cancel as FailIcon,
} from '@mui/icons-material'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { TestQueryResult } from '../types'

export default function TestQueries() {
  const [queries, setQueries] = useState<string[]>([''])
  const [results, setResults] = useState<TestQueryResult[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const addQuery = () => setQueries([...queries, ''])

  const removeQuery = (idx: number) => {
    if (queries.length <= 1) return
    setQueries(queries.filter((_, i) => i !== idx))
  }

  const updateQuery = (idx: number, val: string) => {
    setQueries(queries.map((q, i) => (i === idx ? val : q)))
  }

  const handleRunTests = async () => {
    const validQueries = queries.filter((q) => q.trim())
    if (validQueries.length === 0) return
    setLoading(true)
    setError('')
    setResults([])
    try {
      const res = await api.runTestQueries({ queries: validQueries })
      setResults(Array.isArray(res) ? res : [])
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Test execution failed')
    } finally {
      setLoading(false)
    }
  }

  const passCount = results.filter((r) => r.match).length
  const failCount = results.filter((r) => !r.match).length

  return (
    <Box>
      <Grid container spacing={3}>
        {/* Query Input */}
        <Grid item xs={12} md={5}>
          <motion.div initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
                  <Typography variant="h6">Test Queries</Typography>
                  <Chip size="small" label={`${queries.filter((q) => q.trim()).length} queries`} />
                </Box>

                {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  {queries.map((query, idx) => (
                    <Box key={idx} sx={{ display: 'flex', gap: 1 }}>
                      <TextField
                        multiline
                        rows={3}
                        fullWidth
                        value={query}
                        onChange={(e) => updateQuery(idx, e.target.value)}
                        placeholder={`-- Query ${idx + 1}\nSELECT COUNT(*) FROM customers WHERE active = 1`}
                        sx={{
                          '& .MuiInputBase-root': {
                            fontFamily: '"Fira Code", "Cascadia Code", monospace',
                            fontSize: 12,
                            bgcolor: '#1B1B1F',
                          },
                        }}
                      />
                      <IconButton
                        size="small"
                        color="error"
                        onClick={() => removeQuery(idx)}
                        disabled={queries.length <= 1}
                        sx={{ alignSelf: 'flex-start' }}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Box>
                  ))}

                  <Box sx={{ display: 'flex', gap: 1 }}>
                    <Button
                      size="small"
                      startIcon={<AddIcon />}
                      onClick={addQuery}
                      variant="outlined"
                    >
                      Add Query
                    </Button>
                  </Box>

                  <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                    <Button
                      variant="contained"
                      fullWidth
                      startIcon={loading ? <CircularProgress size={18} color="inherit" /> : <TestIcon />}
                      onClick={handleRunTests}
                      disabled={loading || queries.every((q) => !q.trim())}
                    >
                      {loading ? 'Running Tests...' : 'Run Comparison Tests'}
                    </Button>
                  </motion.div>
                </Box>
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Results */}
        <Grid item xs={12} md={7}>
          <motion.div initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: 0.1 }}>
            {results.length > 0 ? (
              <Card>
                <CardContent>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 2 }}>
                    <Typography variant="h6">Test Results</Typography>
                    <Chip
                      size="small"
                      icon={<PassIcon fontSize="small" />}
                      label={`${passCount} passed`}
                      color="success"
                    />
                    {failCount > 0 && (
                      <Chip
                        size="small"
                        icon={<FailIcon fontSize="small" />}
                        label={`${failCount} failed`}
                        color="error"
                      />
                    )}
                  </Box>

                  <TableContainer>
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>Status</TableCell>
                          <TableCell>Query</TableCell>
                          <TableCell>Source Result</TableCell>
                          <TableCell>Target Result</TableCell>
                          <TableCell align="right">Time (ms)</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {results.map((result, idx) => (
                          <TableRow
                            key={idx}
                            hover
                            sx={{
                              bgcolor: result.match
                                ? 'rgba(0, 169, 114, 0.04)'
                                : 'rgba(255, 54, 33, 0.04)',
                            }}
                          >
                            <TableCell>
                              {result.match ? (
                                <PassIcon fontSize="small" color="success" />
                              ) : (
                                <FailIcon fontSize="small" color="error" />
                              )}
                            </TableCell>
                            <TableCell
                              sx={{
                                fontFamily: 'monospace',
                                fontSize: 12,
                                maxWidth: 200,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                              }}
                            >
                              {result.query}
                            </TableCell>
                            <TableCell sx={{ fontFamily: 'monospace', fontSize: 12 }}>
                              {result.source_result}
                            </TableCell>
                            <TableCell
                              sx={{
                                fontFamily: 'monospace',
                                fontSize: 12,
                                color: result.match ? 'success.main' : 'error.main',
                              }}
                            >
                              {result.target_result}
                            </TableCell>
                            <TableCell align="right">
                              <Chip
                                size="small"
                                label={`${result.execution_time_ms}ms`}
                                variant="outlined"
                              />
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                </CardContent>
              </Card>
            ) : (
              <Card sx={{ height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <Box sx={{ textAlign: 'center' }}>
                  <TestIcon sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
                  <Typography color="text.secondary">
                    Add queries and run comparison tests to validate migration accuracy.
                  </Typography>
                  <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                    Each query runs against both source and Databricks, then compares results.
                  </Typography>
                </Box>
              </Card>
            )}
          </motion.div>
        </Grid>
      </Grid>
    </Box>
  )
}
