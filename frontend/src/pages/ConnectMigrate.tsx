import { useState } from 'react'
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
  Stepper,
  Step,
  StepLabel,
  IconButton,
} from '@mui/material'
import {
  Cable as ConnectIcon,
  CheckCircle as CheckIcon,
  Inventory as InventoryIcon,
  Visibility as ViewIcon,
  VisibilityOff as HideIcon,
} from '@mui/icons-material'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { ConnectionConfig, ConnectionTestResult, InventoryItem } from '../types'

const SOURCE_TYPES = [
  { value: 'sqlserver', label: 'SQL Server', port: 1433 },
  { value: 'snowflake', label: 'Snowflake', port: 443 },
  { value: 'postgresql', label: 'PostgreSQL', port: 5432 },
  { value: 'mysql', label: 'MySQL', port: 3306 },
  { value: 'oracle', label: 'Oracle', port: 1521 },
  { value: 'redshift', label: 'Redshift', port: 5439 },
]

export default function ConnectMigrate() {
  const [activeStep, setActiveStep] = useState(0)
  const [config, setConfig] = useState<ConnectionConfig>({
    source_type: 'sqlserver',
    host: '',
    port: 1433,
    database: '',
    username: '',
    password: '',
    schema: '',
  })
  const [showPassword, setShowPassword] = useState(false)
  const [testResult, setTestResult] = useState<ConnectionTestResult | null>(null)
  const [inventory, setInventory] = useState<InventoryItem[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const updateConfig = (field: keyof ConnectionConfig, value: string | number) => {
    setConfig((prev) => ({ ...prev, [field]: value }))
  }

  const handleSourceTypeChange = (sourceType: string) => {
    const src = SOURCE_TYPES.find((s) => s.value === sourceType)
    setConfig((prev) => ({
      ...prev,
      source_type: sourceType,
      port: src?.port ?? prev.port,
    }))
    setTestResult(null)
    setInventory([])
  }

  const handleTestConnection = async () => {
    setLoading(true)
    setError('')
    setTestResult(null)
    try {
      const result = await api.testConnection(config)
      setTestResult(result)
      if (result.success) setActiveStep(1)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Connection test failed')
    } finally {
      setLoading(false)
    }
  }

  const handleExtractInventory = async () => {
    setLoading(true)
    setError('')
    try {
      const items = await api.extractInventory(config)
      setInventory(Array.isArray(items) ? items : [])
      setActiveStep(2)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to extract inventory')
    } finally {
      setLoading(false)
    }
  }

  return (
    <Box>
      <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
        <Step><StepLabel>Configure Connection</StepLabel></Step>
        <Step><StepLabel>Test Connection</StepLabel></Step>
        <Step><StepLabel>Extract Inventory</StepLabel></Step>
      </Stepper>

      <Grid container spacing={3}>
        {/* Connection Form */}
        <Grid item xs={12} md={5}>
          <motion.div initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Source Connection
                </Typography>

                {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5 }}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Source Type</InputLabel>
                    <Select
                      value={config.source_type}
                      label="Source Type"
                      onChange={(e) => handleSourceTypeChange(e.target.value)}
                    >
                      {SOURCE_TYPES.map((s) => (
                        <MenuItem key={s.value} value={s.value}>{s.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>

                  <TextField
                    size="small"
                    label="Host / Server"
                    value={config.host}
                    onChange={(e) => updateConfig('host', e.target.value)}
                    placeholder="e.g., myserver.database.windows.net"
                    fullWidth
                  />

                  <Box sx={{ display: 'flex', gap: 2 }}>
                    <TextField
                      size="small"
                      label="Port"
                      type="number"
                      value={config.port}
                      onChange={(e) => updateConfig('port', parseInt(e.target.value) || 0)}
                      sx={{ width: 120 }}
                    />
                    <TextField
                      size="small"
                      label="Database"
                      value={config.database}
                      onChange={(e) => updateConfig('database', e.target.value)}
                      fullWidth
                    />
                  </Box>

                  <TextField
                    size="small"
                    label="Schema (optional)"
                    value={config.schema}
                    onChange={(e) => updateConfig('schema', e.target.value)}
                    placeholder="dbo"
                    fullWidth
                  />

                  <TextField
                    size="small"
                    label="Username"
                    value={config.username}
                    onChange={(e) => updateConfig('username', e.target.value)}
                    fullWidth
                  />

                  <TextField
                    size="small"
                    label="Password"
                    type={showPassword ? 'text' : 'password'}
                    value={config.password}
                    onChange={(e) => updateConfig('password', e.target.value)}
                    fullWidth
                    InputProps={{
                      endAdornment: (
                        <IconButton size="small" onClick={() => setShowPassword(!showPassword)}>
                          {showPassword ? <HideIcon fontSize="small" /> : <ViewIcon fontSize="small" />}
                        </IconButton>
                      ),
                    }}
                  />

                  <Box sx={{ display: 'flex', gap: 1 }}>
                    <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                      <Button
                        variant="contained"
                        startIcon={loading && !testResult ? <CircularProgress size={18} color="inherit" /> : <ConnectIcon />}
                        onClick={handleTestConnection}
                        disabled={loading || !config.host || !config.database}
                      >
                        Test Connection
                      </Button>
                    </motion.div>

                    {testResult?.success && (
                      <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                        <Button
                          variant="outlined"
                          startIcon={loading && testResult ? <CircularProgress size={18} /> : <InventoryIcon />}
                          onClick={handleExtractInventory}
                          disabled={loading}
                        >
                          Extract Inventory
                        </Button>
                      </motion.div>
                    )}
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Connection Result + Inventory */}
        <Grid item xs={12} md={7}>
          <motion.div initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: 0.1 }}>
            {testResult && (
              <Card sx={{ mb: 3 }}>
                <CardContent>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                    <CheckIcon color={testResult.success ? 'success' : 'error'} />
                    <Typography variant="h6">
                      {testResult.success ? 'Connection Successful' : 'Connection Failed'}
                    </Typography>
                  </Box>
                  <Typography color="text.secondary" sx={{ mt: 1 }}>
                    {testResult.message}
                  </Typography>
                  {testResult.server_version && (
                    <Chip
                      size="small"
                      label={`Server: ${testResult.server_version}`}
                      sx={{ mt: 1 }}
                    />
                  )}
                </CardContent>
              </Card>
            )}

            {inventory.length > 0 && (
              <Card>
                <CardContent>
                  <Typography variant="h6" gutterBottom>
                    Source Inventory
                    <Chip size="small" label={`${inventory.length} objects`} sx={{ ml: 1 }} />
                  </Typography>
                  <TableContainer sx={{ maxHeight: 480 }}>
                    <Table size="small" stickyHeader>
                      <TableHead>
                        <TableRow>
                          <TableCell>Name</TableCell>
                          <TableCell>Type</TableCell>
                          <TableCell>Schema</TableCell>
                          <TableCell align="right">Rows</TableCell>
                          <TableCell align="right">Size (MB)</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {inventory.map((item) => (
                          <TableRow key={`${item.schema}.${item.name}`} hover>
                            <TableCell sx={{ fontFamily: 'monospace', fontSize: 13 }}>
                              {item.name}
                            </TableCell>
                            <TableCell>
                              <Chip
                                size="small"
                                label={item.type}
                                color={item.type === 'TABLE' ? 'primary' : 'default'}
                                variant="outlined"
                              />
                            </TableCell>
                            <TableCell>{item.schema}</TableCell>
                            <TableCell align="right">
                              {item.row_count?.toLocaleString() ?? '--'}
                            </TableCell>
                            <TableCell align="right">
                              {item.size_mb?.toFixed(1) ?? '--'}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                </CardContent>
              </Card>
            )}

            {!testResult && !inventory.length && (
              <Card sx={{ height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <CardContent sx={{ textAlign: 'center' }}>
                  <ConnectIcon sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
                  <Typography color="text.secondary">
                    Configure your source database connection and click "Test Connection" to begin.
                  </Typography>
                </CardContent>
              </Card>
            )}
          </motion.div>
        </Grid>
      </Grid>
    </Box>
  )
}
