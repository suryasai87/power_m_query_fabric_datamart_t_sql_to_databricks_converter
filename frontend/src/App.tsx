import { useState } from 'react'
import {
  ThemeProvider,
  CssBaseline,
  AppBar,
  Toolbar,
  Typography,
  Tabs,
  Tab,
  Box,
  Container,
  Chip,
} from '@mui/material'
import {
  Dashboard as DashboardIcon,
  Code as CodeIcon,
  TableChart as TableIcon,
  Storage as StorageIcon,
  CloudSync as CloudSyncIcon,
  Schedule as ScheduleIcon,
  CompareArrows as CompareIcon,
  AttachMoney as MoneyIcon,
  PlaylistAddCheck as TestIcon,
  Restore as RestoreIcon,
  History as HistoryIcon,
} from '@mui/icons-material'
import { AnimatePresence, motion } from 'framer-motion'
import theme from './theme'
import Dashboard from './pages/Dashboard'
import SqlTranslator from './pages/SqlTranslator'
import ConvertDdl from './pages/ConvertDdl'
import ConnectMigrate from './pages/ConnectMigrate'
import BulkMigration from './pages/BulkMigration'
import Scheduler from './pages/Scheduler'
import SchemaCompare from './pages/SchemaCompare'
import CostEstimator from './pages/CostEstimator'
import TestQueries from './pages/TestQueries'
import RollbackManager from './pages/RollbackManager'
import MigrationHistory from './pages/MigrationHistory'

const tabs = [
  { label: 'Dashboard', icon: <DashboardIcon /> },
  { label: 'SQL Translator', icon: <CodeIcon /> },
  { label: 'Convert DDL', icon: <TableIcon /> },
  { label: 'Connect & Migrate', icon: <StorageIcon /> },
  { label: 'Bulk Migration', icon: <CloudSyncIcon /> },
  { label: 'Scheduler', icon: <ScheduleIcon /> },
  { label: 'Schema Compare', icon: <CompareIcon /> },
  { label: 'Cost Estimator', icon: <MoneyIcon /> },
  { label: 'Test Queries', icon: <TestIcon /> },
  { label: 'Rollback', icon: <RestoreIcon /> },
  { label: 'History', icon: <HistoryIcon /> },
]

const pageVariants = {
  initial: { opacity: 0, y: 16 },
  animate: { opacity: 1, y: 0, transition: { duration: 0.3, ease: 'easeOut' } },
  exit: { opacity: 0, y: -12, transition: { duration: 0.2 } },
}

function TabPanel({ children, value, index }: { children: React.ReactNode; value: number; index: number }) {
  if (value !== index) return null
  return (
    <motion.div
      key={index}
      variants={pageVariants}
      initial="initial"
      animate="animate"
      exit="exit"
      style={{ width: '100%' }}
    >
      {children}
    </motion.div>
  )
}

export default function App() {
  const [tab, setTab] = useState(0)

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
        <AppBar
          position="sticky"
          elevation={0}
          sx={{
            bgcolor: 'background.paper',
            borderBottom: '1px solid',
            borderColor: 'divider',
          }}
        >
          <Toolbar sx={{ gap: 2 }}>
            <Box
              sx={{
                width: 36,
                height: 36,
                borderRadius: '8px',
                bgcolor: 'primary.main',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontWeight: 800,
                fontSize: 18,
                color: '#fff',
              }}
            >
              DW
            </Box>
            <Box sx={{ flexGrow: 0 }}>
              <Typography variant="h6" sx={{ color: 'text.primary', lineHeight: 1.2 }}>
                DW Migration Assistant
              </Typography>
              <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                Power BI / Fabric Datamart T-SQL to Databricks Converter
              </Typography>
            </Box>
            <Box sx={{ flexGrow: 1 }} />
            <Chip
              label="Databricks"
              size="small"
              sx={{
                bgcolor: 'rgba(255, 54, 33, 0.12)',
                color: 'primary.main',
                fontWeight: 600,
              }}
            />
          </Toolbar>
          <Tabs
            value={tab}
            onChange={(_, v) => setTab(v)}
            variant="scrollable"
            scrollButtons="auto"
            sx={{
              px: 2,
              '& .MuiTab-root': { minWidth: 'auto', px: 2 },
              '& .MuiTabs-indicator': { height: 3, borderRadius: '3px 3px 0 0' },
            }}
          >
            {tabs.map((t, i) => (
              <Tab key={i} icon={t.icon} label={t.label} iconPosition="start" />
            ))}
          </Tabs>
        </AppBar>

        <Container maxWidth="xl" sx={{ py: 3 }}>
          <AnimatePresence mode="wait">
            <TabPanel value={tab} index={0}><Dashboard /></TabPanel>
            <TabPanel value={tab} index={1}><SqlTranslator /></TabPanel>
            <TabPanel value={tab} index={2}><ConvertDdl /></TabPanel>
            <TabPanel value={tab} index={3}><ConnectMigrate /></TabPanel>
            <TabPanel value={tab} index={4}><BulkMigration /></TabPanel>
            <TabPanel value={tab} index={5}><Scheduler /></TabPanel>
            <TabPanel value={tab} index={6}><SchemaCompare /></TabPanel>
            <TabPanel value={tab} index={7}><CostEstimator /></TabPanel>
            <TabPanel value={tab} index={8}><TestQueries /></TabPanel>
            <TabPanel value={tab} index={9}><RollbackManager /></TabPanel>
            <TabPanel value={tab} index={10}><MigrationHistory /></TabPanel>
          </AnimatePresence>
        </Container>
      </Box>
    </ThemeProvider>
  )
}
