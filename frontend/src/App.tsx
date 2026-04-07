import { useState } from 'react'
import {
  ThemeProvider,
  CssBaseline,
  Typography,
  Box,
  Drawer,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Chip,
  Toolbar,
  AppBar,
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

const DRAWER_WIDTH = 240

const navItems = [
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

const pages = [
  <Dashboard />,
  <SqlTranslator />,
  <ConvertDdl />,
  <ConnectMigrate />,
  <BulkMigration />,
  <Scheduler />,
  <SchemaCompare />,
  <CostEstimator />,
  <TestQueries />,
  <RollbackManager />,
  <MigrationHistory />,
]

const pageVariants = {
  initial: { opacity: 0, y: 16 },
  animate: { opacity: 1, y: 0, transition: { duration: 0.3, ease: 'easeOut' } },
  exit: { opacity: 0, y: -12, transition: { duration: 0.2 } },
}

export default function App() {
  const [selected, setSelected] = useState(0)

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex', minHeight: '100vh' }}>
        {/* Sidebar */}
        <Drawer
          variant="permanent"
          sx={{
            width: DRAWER_WIDTH,
            flexShrink: 0,
            '& .MuiDrawer-paper': {
              width: DRAWER_WIDTH,
              boxSizing: 'border-box',
              bgcolor: 'background.paper',
              borderRight: '1px solid',
              borderColor: 'divider',
            },
          }}
        >
          {/* Logo / Brand */}
          <Box sx={{ px: 2.5, py: 2.5, borderBottom: '1px solid', borderColor: 'divider' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
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
                  flexShrink: 0,
                }}
              >
                DW
              </Box>
              <Box sx={{ minWidth: 0 }}>
                <Typography variant="subtitle2" sx={{ fontWeight: 700, lineHeight: 1.2 }} noWrap>
                  DW Migration Assistant
                </Typography>
                <Typography variant="caption" color="text.secondary" noWrap>
                  Power BI / Fabric / T-SQL to Databricks Converter
                </Typography>
              </Box>
            </Box>
          </Box>

          {/* Navigation Items */}
          <List sx={{ px: 1, py: 1.5, flex: 1 }}>
            {navItems.map((item, i) => (
              <ListItemButton
                key={item.label}
                selected={selected === i}
                onClick={() => setSelected(i)}
                sx={{
                  borderRadius: '8px',
                  mb: 0.3,
                  py: 0.8,
                  '&.Mui-selected': {
                    bgcolor: 'rgba(255, 54, 33, 0.10)',
                    '&:hover': { bgcolor: 'rgba(255, 54, 33, 0.16)' },
                    '& .MuiListItemIcon-root': { color: 'primary.main' },
                    '& .MuiListItemText-primary': { fontWeight: 600, color: 'primary.main' },
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 36 }}>{item.icon}</ListItemIcon>
                <ListItemText
                  primary={item.label}
                  primaryTypographyProps={{ variant: 'body2' }}
                />
              </ListItemButton>
            ))}
          </List>

          {/* Bottom badge */}
          <Box sx={{ p: 2, borderTop: '1px solid', borderColor: 'divider', textAlign: 'center' }}>
            <Chip
              label="Databricks"
              size="small"
              sx={{
                bgcolor: 'rgba(255, 54, 33, 0.12)',
                color: 'primary.main',
                fontWeight: 600,
              }}
            />
          </Box>
        </Drawer>

        {/* Main Content */}
        <Box
          component="main"
          sx={{
            flexGrow: 1,
            bgcolor: 'background.default',
            p: 3,
            minHeight: '100vh',
            overflow: 'auto',
          }}
        >
          <AnimatePresence mode="wait">
            <motion.div
              key={selected}
              variants={pageVariants}
              initial="initial"
              animate="animate"
              exit="exit"
              style={{ width: '100%' }}
            >
              {pages[selected]}
            </motion.div>
          </AnimatePresence>
        </Box>
      </Box>
    </ThemeProvider>
  )
}
