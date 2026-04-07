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
  IconButton,
  Tooltip,
} from '@mui/material'
import {
  Translate as TranslateIcon,
  PlayArrow as RunIcon,
  ContentCopy as CopyIcon,
  Clear as ClearIcon,
} from '@mui/icons-material'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { ModelInfo, TranslateSqlResponse } from '../types'

export default function SqlTranslator() {
  const [sourceSql, setSourceSql] = useState('')
  const [sourceDialect, setSourceDialect] = useState('tsql')
  const [model, setModel] = useState('')
  const [models, setModels] = useState<ModelInfo[]>([])
  const [result, setResult] = useState<TranslateSqlResponse | null>(null)
  const [execResult, setExecResult] = useState<{ results: Record<string, unknown>[]; row_count: number } | null>(null)
  const [loading, setLoading] = useState(false)
  const [executing, setExecuting] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    api.getModels().then(d => setModels(Array.isArray(d) ? d : [])).catch(() => {
      setModels([
        { id: 'gpt-4', name: 'GPT-4', provider: 'OpenAI' },
        { id: 'claude-sonnet', name: 'Claude Sonnet', provider: 'Anthropic' },
      ])
    })
  }, [])

  const handleTranslate = async () => {
    if (!sourceSql.trim()) return
    setLoading(true)
    setError('')
    setResult(null)
    setExecResult(null)
    try {
      const res = await api.translateSql({
        source_sql: sourceSql,
        source_dialect: sourceDialect,
        model: model || undefined,
      })
      setResult(res)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Translation failed')
    } finally {
      setLoading(false)
    }
  }

  const handleExecute = async () => {
    if (!result?.translated_sql) return
    setExecuting(true)
    setError('')
    try {
      const res = await api.executeSql(result.translated_sql)
      setExecResult(res)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Execution failed')
    } finally {
      setExecuting(false)
    }
  }

  const handleCopy = () => {
    if (result?.translated_sql) {
      navigator.clipboard.writeText(result.translated_sql)
    }
  }

  return (
    <Box>
      <Grid container spacing={3}>
        {/* Source SQL */}
        <Grid item xs={12} md={6}>
          <motion.div initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }} transition={{ duration: 0.3 }}>
            <Card sx={{ height: '100%' }}>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
                  <Typography variant="h6">Source SQL</Typography>
                  <IconButton size="small" onClick={() => { setSourceSql(''); setResult(null); setExecResult(null) }}>
                    <ClearIcon fontSize="small" />
                  </IconButton>
                </Box>

                <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
                  <FormControl size="small" sx={{ minWidth: 140 }}>
                    <InputLabel>Source Dialect</InputLabel>
                    <Select
                      value={sourceDialect}
                      label="Source Dialect"
                      onChange={(e) => setSourceDialect(e.target.value)}
                    >
                      <MenuItem value="tsql">T-SQL</MenuItem>
                      <MenuItem value="mysql">MySQL</MenuItem>
                      <MenuItem value="postgresql">PostgreSQL</MenuItem>
                      <MenuItem value="snowflake">Snowflake</MenuItem>
                      <MenuItem value="oracle">Oracle</MenuItem>
                      <MenuItem value="redshift">Redshift</MenuItem>
                      <MenuItem value="power_query_m">Power Query M</MenuItem>
                    </Select>
                  </FormControl>

                  <FormControl size="small" sx={{ minWidth: 160 }}>
                    <InputLabel>AI Model</InputLabel>
                    <Select
                      value={model}
                      label="AI Model"
                      onChange={(e) => setModel(e.target.value)}
                    >
                      <MenuItem value="">Default</MenuItem>
                      {models.map((m) => (
                        <MenuItem key={m.id} value={m.id}>{m.name}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Box>

                <TextField
                  multiline
                  rows={16}
                  fullWidth
                  value={sourceSql}
                  onChange={(e) => setSourceSql(e.target.value)}
                  placeholder={`-- Paste your ${sourceDialect.toUpperCase()} query here\nSELECT TOP 100\n  customer_id,\n  CONVERT(VARCHAR, order_date, 23) AS order_date,\n  ISNULL(amount, 0) AS amount\nFROM dbo.orders\nWHERE order_date >= DATEADD(day, -30, GETDATE())`}
                  sx={{
                    '& .MuiInputBase-root': {
                      fontFamily: '"Fira Code", "Cascadia Code", monospace',
                      fontSize: 13,
                      bgcolor: '#1B1B1F',
                    },
                  }}
                />

                <Box sx={{ mt: 2, display: 'flex', gap: 1 }}>
                  <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                    <Button
                      variant="contained"
                      startIcon={loading ? <CircularProgress size={18} color="inherit" /> : <TranslateIcon />}
                      onClick={handleTranslate}
                      disabled={loading || !sourceSql.trim()}
                    >
                      {loading ? 'Translating...' : 'Translate to Databricks SQL'}
                    </Button>
                  </motion.div>
                </Box>
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Translated SQL */}
        <Grid item xs={12} md={6}>
          <motion.div initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} transition={{ duration: 0.3, delay: 0.1 }}>
            <Card sx={{ height: '100%' }}>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
                  <Typography variant="h6">Databricks SQL</Typography>
                  <Box sx={{ display: 'flex', gap: 1 }}>
                    {result && (
                      <>
                        <Chip
                          size="small"
                          label={`Model: ${result.model_used}`}
                          sx={{ bgcolor: 'rgba(33, 150, 243, 0.12)', color: '#2196F3' }}
                        />
                        <Tooltip title="Copy to clipboard">
                          <IconButton size="small" onClick={handleCopy}>
                            <CopyIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      </>
                    )}
                  </Box>
                </Box>

                {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

                {result?.warnings && result.warnings.length > 0 && (
                  <Alert severity="warning" sx={{ mb: 2 }}>
                    {result.warnings.map((w, i) => <div key={i}>{w}</div>)}
                  </Alert>
                )}

                <TextField
                  multiline
                  rows={16}
                  fullWidth
                  value={result?.translated_sql ?? ''}
                  placeholder="-- Translated SQL will appear here"
                  InputProps={{ readOnly: true }}
                  sx={{
                    '& .MuiInputBase-root': {
                      fontFamily: '"Fira Code", "Cascadia Code", monospace',
                      fontSize: 13,
                      bgcolor: '#1B1B1F',
                    },
                  }}
                />

                <Box sx={{ mt: 2, display: 'flex', gap: 1 }}>
                  <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                    <Button
                      variant="outlined"
                      startIcon={executing ? <CircularProgress size={18} /> : <RunIcon />}
                      onClick={handleExecute}
                      disabled={executing || !result?.translated_sql}
                    >
                      {executing ? 'Executing...' : 'Execute on Databricks'}
                    </Button>
                  </motion.div>
                </Box>
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Execution Results */}
        {execResult && (
          <Grid item xs={12}>
            <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}>
              <Card>
                <CardContent>
                  <Typography variant="h6" gutterBottom>
                    Execution Results
                    <Chip size="small" label={`${execResult.row_count} rows`} sx={{ ml: 1 }} />
                  </Typography>
                  <Box sx={{ overflowX: 'auto' }}>
                    <pre style={{ fontFamily: 'monospace', fontSize: 12, color: '#B0B0B8' }}>
                      {JSON.stringify(execResult.results, null, 2)}
                    </pre>
                  </Box>
                </CardContent>
              </Card>
            </motion.div>
          </Grid>
        )}
      </Grid>
    </Box>
  )
}
