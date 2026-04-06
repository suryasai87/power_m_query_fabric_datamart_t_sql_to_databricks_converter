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
  IconButton,
  Tooltip,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
} from '@mui/material'
import {
  Transform as ConvertIcon,
  ContentCopy as CopyIcon,
  Clear as ClearIcon,
  TableChart as TableIcon,
} from '@mui/icons-material'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { ConvertDdlResponse } from '../types'

export default function ConvertDdl() {
  const [ddl, setDdl] = useState('')
  const [catalog, setCatalog] = useState('main')
  const [schema, setSchema] = useState('default')
  const [result, setResult] = useState<ConvertDdlResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const handleConvert = async () => {
    if (!ddl.trim()) return
    setLoading(true)
    setError('')
    setResult(null)
    try {
      const res = await api.convertDdl({
        ddl,
        target_catalog: catalog || undefined,
        target_schema: schema || undefined,
      })
      setResult(res)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'DDL conversion failed')
    } finally {
      setLoading(false)
    }
  }

  const handleCopy = () => {
    if (result?.converted_ddl) {
      navigator.clipboard.writeText(result.converted_ddl)
    }
  }

  return (
    <Box>
      <Grid container spacing={3}>
        {/* Input DDL */}
        <Grid size={{ xs: 12, md: 6 }}>
          <motion.div initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }} transition={{ duration: 0.3 }}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
                  <Typography variant="h6">Source DDL</Typography>
                  <IconButton size="small" onClick={() => { setDdl(''); setResult(null) }}>
                    <ClearIcon fontSize="small" />
                  </IconButton>
                </Box>

                <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
                  <TextField
                    size="small"
                    label="Target Catalog"
                    value={catalog}
                    onChange={(e) => setCatalog(e.target.value)}
                    sx={{ width: 180 }}
                  />
                  <TextField
                    size="small"
                    label="Target Schema"
                    value={schema}
                    onChange={(e) => setSchema(e.target.value)}
                    sx={{ width: 180 }}
                  />
                </Box>

                <TextField
                  multiline
                  rows={18}
                  fullWidth
                  value={ddl}
                  onChange={(e) => setDdl(e.target.value)}
                  placeholder={`-- Paste your CREATE TABLE statements here\nCREATE TABLE dbo.customers (\n  customer_id INT IDENTITY(1,1) PRIMARY KEY,\n  first_name NVARCHAR(100) NOT NULL,\n  last_name NVARCHAR(100) NOT NULL,\n  email VARCHAR(255) UNIQUE,\n  created_date DATETIME2 DEFAULT GETDATE(),\n  is_active BIT DEFAULT 1\n);\n\nCREATE TABLE dbo.orders (\n  order_id INT IDENTITY(1,1) PRIMARY KEY,\n  customer_id INT REFERENCES dbo.customers(customer_id),\n  order_date DATETIME2 NOT NULL,\n  total_amount MONEY,\n  status NVARCHAR(50)\n);`}
                  sx={{
                    '& .MuiInputBase-root': {
                      fontFamily: '"Fira Code", "Cascadia Code", monospace',
                      fontSize: 13,
                      bgcolor: '#1B1B1F',
                    },
                  }}
                />

                <Box sx={{ mt: 2 }}>
                  <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                    <Button
                      variant="contained"
                      startIcon={loading ? <CircularProgress size={18} color="inherit" /> : <ConvertIcon />}
                      onClick={handleConvert}
                      disabled={loading || !ddl.trim()}
                    >
                      {loading ? 'Converting...' : 'Convert to Databricks DDL'}
                    </Button>
                  </motion.div>
                </Box>
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Converted DDL */}
        <Grid size={{ xs: 12, md: 6 }}>
          <motion.div initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} transition={{ duration: 0.3, delay: 0.1 }}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
                  <Typography variant="h6">Databricks DDL (Unity Catalog)</Typography>
                  {result && (
                    <Tooltip title="Copy to clipboard">
                      <IconButton size="small" onClick={handleCopy}>
                        <CopyIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  )}
                </Box>

                {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

                {result?.warnings && result.warnings.length > 0 && (
                  <Alert severity="warning" sx={{ mb: 2 }}>
                    {result.warnings.map((w, i) => <div key={i}>{w}</div>)}
                  </Alert>
                )}

                <TextField
                  multiline
                  rows={18}
                  fullWidth
                  value={result?.converted_ddl ?? ''}
                  placeholder="-- Converted DDL will appear here"
                  slotProps={{ input: { readOnly: true } }}
                  sx={{
                    '& .MuiInputBase-root': {
                      fontFamily: '"Fira Code", "Cascadia Code", monospace',
                      fontSize: 13,
                      bgcolor: '#1B1B1F',
                    },
                  }}
                />

                {result?.tables_created && result.tables_created.length > 0 && (
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="subtitle2" gutterBottom>
                      Tables Created
                    </Typography>
                    <List dense>
                      {result.tables_created.map((table) => (
                        <ListItem key={table}>
                          <ListItemIcon sx={{ minWidth: 36 }}>
                            <TableIcon fontSize="small" color="secondary" />
                          </ListItemIcon>
                          <ListItemText primary={table} />
                          <Chip size="small" label="Delta" color="success" variant="outlined" />
                        </ListItem>
                      ))}
                    </List>
                  </Box>
                )}
              </CardContent>
            </Card>
          </motion.div>
        </Grid>
      </Grid>
    </Box>
  )
}
