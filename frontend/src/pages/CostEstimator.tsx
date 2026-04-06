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
  Slider,
  Divider,
} from '@mui/material'
import {
  Calculate as CalcIcon,
  Storage as StorageIcon,
  Speed as ComputeIcon,
  CloudSync as MigrateIcon,
} from '@mui/icons-material'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts'
import { motion } from 'framer-motion'
import { api } from '../api'
import type { CostEstimate } from '../types'

const COLORS = ['#FF3621', '#2196F3', '#00A972', '#FFB020']

export default function CostEstimator() {
  const [storageGb, setStorageGb] = useState(100)
  const [dailyQueries, setDailyQueries] = useState(500)
  const [tables, setTables] = useState('10')
  const [estimate, setEstimate] = useState<CostEstimate | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const handleEstimate = async () => {
    setLoading(true)
    setError('')
    try {
      const result = await api.estimateCost({
        tables: Array.from({ length: parseInt(tables) || 1 }, (_, i) => `table_${i + 1}`),
        storage_gb: storageGb,
        daily_queries: dailyQueries,
      })
      setEstimate(result)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Cost estimation failed')
    } finally {
      setLoading(false)
    }
  }

  const comparisonData = estimate
    ? [
        {
          platform: 'Fabric Datamart',
          storage: estimate.storage_cost * 1.4,
          compute: estimate.compute_cost * 1.8,
          migration: 0,
        },
        {
          platform: 'Databricks',
          storage: estimate.storage_cost,
          compute: estimate.compute_cost,
          migration: estimate.migration_cost,
        },
        {
          platform: 'Snowflake',
          storage: estimate.storage_cost * 1.2,
          compute: estimate.compute_cost * 1.5,
          migration: estimate.migration_cost * 1.3,
        },
      ]
    : []

  const breakdownData = estimate
    ? [
        { name: 'Storage', value: estimate.storage_cost },
        { name: 'Compute', value: estimate.compute_cost },
        { name: 'Migration', value: estimate.migration_cost },
      ]
    : []

  return (
    <Box>
      <Grid container spacing={3}>
        {/* Input Form */}
        <Grid size={{ xs: 12, md: 4 }}>
          <motion.div initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Cost Parameters
                </Typography>

                {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
                  <Box>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                      <StorageIcon fontSize="small" color="primary" />
                      <Typography variant="subtitle2">Storage (GB)</Typography>
                    </Box>
                    <Slider
                      value={storageGb}
                      onChange={(_, v) => setStorageGb(v as number)}
                      min={1}
                      max={10000}
                      step={10}
                      valueLabelDisplay="auto"
                      valueLabelFormat={(v) => `${v} GB`}
                    />
                    <TextField
                      size="small"
                      type="number"
                      value={storageGb}
                      onChange={(e) => setStorageGb(parseInt(e.target.value) || 0)}
                      slotProps={{ htmlInput: { min: 1, max: 10000 } }}
                      fullWidth
                    />
                  </Box>

                  <Box>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                      <ComputeIcon fontSize="small" color="secondary" />
                      <Typography variant="subtitle2">Daily Queries</Typography>
                    </Box>
                    <Slider
                      value={dailyQueries}
                      onChange={(_, v) => setDailyQueries(v as number)}
                      min={1}
                      max={10000}
                      step={50}
                      valueLabelDisplay="auto"
                    />
                    <TextField
                      size="small"
                      type="number"
                      value={dailyQueries}
                      onChange={(e) => setDailyQueries(parseInt(e.target.value) || 0)}
                      fullWidth
                    />
                  </Box>

                  <Box>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                      <MigrateIcon fontSize="small" color="info" />
                      <Typography variant="subtitle2">Number of Tables</Typography>
                    </Box>
                    <TextField
                      size="small"
                      type="number"
                      value={tables}
                      onChange={(e) => setTables(e.target.value)}
                      fullWidth
                    />
                  </Box>

                  <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                    <Button
                      variant="contained"
                      fullWidth
                      startIcon={loading ? <CircularProgress size={18} color="inherit" /> : <CalcIcon />}
                      onClick={handleEstimate}
                      disabled={loading}
                    >
                      {loading ? 'Estimating...' : 'Estimate Costs'}
                    </Button>
                  </motion.div>
                </Box>
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Results */}
        <Grid size={{ xs: 12, md: 8 }}>
          {estimate ? (
            <motion.div initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }}>
              <Grid container spacing={3}>
                {/* Cost Summary Cards */}
                <Grid size={{ xs: 6, md: 3 }}>
                  <Card>
                    <CardContent sx={{ textAlign: 'center' }}>
                      <StorageIcon color="primary" />
                      <Typography variant="caption" display="block" color="text.secondary">Storage/mo</Typography>
                      <Typography variant="h6">${estimate.storage_cost.toFixed(0)}</Typography>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid size={{ xs: 6, md: 3 }}>
                  <Card>
                    <CardContent sx={{ textAlign: 'center' }}>
                      <ComputeIcon color="secondary" />
                      <Typography variant="caption" display="block" color="text.secondary">Compute/mo</Typography>
                      <Typography variant="h6">${estimate.compute_cost.toFixed(0)}</Typography>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid size={{ xs: 6, md: 3 }}>
                  <Card>
                    <CardContent sx={{ textAlign: 'center' }}>
                      <MigrateIcon color="info" />
                      <Typography variant="caption" display="block" color="text.secondary">Migration</Typography>
                      <Typography variant="h6">${estimate.migration_cost.toFixed(0)}</Typography>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid size={{ xs: 6, md: 3 }}>
                  <Card sx={{ border: '1px solid', borderColor: 'primary.main' }}>
                    <CardContent sx={{ textAlign: 'center' }}>
                      <CalcIcon color="primary" />
                      <Typography variant="caption" display="block" color="text.secondary">Total/mo</Typography>
                      <Typography variant="h6" color="primary.main">
                        ${estimate.total_monthly.toFixed(0)}
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>

                {/* Platform Comparison Bar Chart */}
                <Grid size={{ xs: 12, md: 7 }}>
                  <Card sx={{ height: 380 }}>
                    <CardContent>
                      <Typography variant="h6" gutterBottom>Platform Cost Comparison</Typography>
                      <ResponsiveContainer width="100%" height={300}>
                        <BarChart data={comparisonData}>
                          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                          <XAxis dataKey="platform" stroke="#B0B0B8" />
                          <YAxis stroke="#B0B0B8" tickFormatter={(v) => `$${v}`} />
                          <Tooltip
                            contentStyle={{
                              backgroundColor: '#2A2A30',
                              border: '1px solid rgba(255,255,255,0.1)',
                              borderRadius: 8,
                              color: '#F5F5F5',
                            }}
                            formatter={(value: number) => [`$${value.toFixed(0)}`, '']}
                          />
                          <Legend />
                          <Bar dataKey="storage" name="Storage" fill="#FF3621" radius={[4, 4, 0, 0]} />
                          <Bar dataKey="compute" name="Compute" fill="#2196F3" radius={[4, 4, 0, 0]} />
                          <Bar dataKey="migration" name="Migration" fill="#00A972" radius={[4, 4, 0, 0]} />
                        </BarChart>
                      </ResponsiveContainer>
                    </CardContent>
                  </Card>
                </Grid>

                {/* Cost Breakdown Pie */}
                <Grid size={{ xs: 12, md: 5 }}>
                  <Card sx={{ height: 380 }}>
                    <CardContent>
                      <Typography variant="h6" gutterBottom>Databricks Cost Breakdown</Typography>
                      <ResponsiveContainer width="100%" height={250}>
                        <PieChart>
                          <Pie
                            data={breakdownData}
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={100}
                            paddingAngle={4}
                            dataKey="value"
                            label={({ name, value }) => `${name}: $${value.toFixed(0)}`}
                          >
                            {breakdownData.map((_, idx) => (
                              <Cell key={idx} fill={COLORS[idx]} />
                            ))}
                          </Pie>
                          <Tooltip
                            contentStyle={{
                              backgroundColor: '#2A2A30',
                              border: '1px solid rgba(255,255,255,0.1)',
                              borderRadius: 8,
                              color: '#F5F5F5',
                            }}
                            formatter={(value: number) => [`$${value.toFixed(2)}`, '']}
                          />
                        </PieChart>
                      </ResponsiveContainer>
                    </CardContent>
                  </Card>
                </Grid>
              </Grid>
            </motion.div>
          ) : (
            <Card sx={{ height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <Box sx={{ textAlign: 'center' }}>
                <CalcIcon sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
                <Typography color="text.secondary">
                  Adjust parameters and click "Estimate Costs" to see a detailed cost comparison.
                </Typography>
              </Box>
            </Card>
          )}
        </Grid>
      </Grid>
    </Box>
  )
}
