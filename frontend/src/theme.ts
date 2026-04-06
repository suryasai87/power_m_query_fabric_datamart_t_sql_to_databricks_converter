import { createTheme } from '@mui/material/styles'

const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: {
      main: '#FF3621',
      light: '#FF6B5A',
      dark: '#CC2B1A',
      contrastText: '#FFFFFF',
    },
    secondary: {
      main: '#00A972',
      light: '#33BA8E',
      dark: '#00875B',
    },
    background: {
      default: '#1B1B1F',
      paper: '#2A2A30',
    },
    text: {
      primary: '#F5F5F5',
      secondary: '#B0B0B8',
    },
    success: {
      main: '#00A972',
    },
    error: {
      main: '#FF3621',
    },
    warning: {
      main: '#FFB020',
    },
    info: {
      main: '#2196F3',
    },
    divider: 'rgba(255, 255, 255, 0.08)',
  },
  typography: {
    fontFamily: '"Inter", "Roboto", "Helvetica", "Arial", sans-serif',
    h4: {
      fontWeight: 700,
      letterSpacing: '-0.02em',
    },
    h5: {
      fontWeight: 600,
    },
    h6: {
      fontWeight: 600,
    },
    subtitle1: {
      color: '#B0B0B8',
    },
  },
  shape: {
    borderRadius: 12,
  },
  components: {
    MuiCard: {
      styleOverrides: {
        root: {
          backgroundImage: 'none',
          border: '1px solid rgba(255, 255, 255, 0.06)',
          boxShadow: '0 4px 24px rgba(0, 0, 0, 0.3)',
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          fontWeight: 600,
          borderRadius: 8,
          padding: '8px 20px',
        },
        contained: {
          boxShadow: '0 2px 8px rgba(255, 54, 33, 0.3)',
          '&:hover': {
            boxShadow: '0 4px 16px rgba(255, 54, 33, 0.4)',
          },
        },
      },
    },
    MuiTextField: {
      styleOverrides: {
        root: {
          '& .MuiOutlinedInput-root': {
            borderRadius: 8,
          },
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          borderRadius: 6,
          fontWeight: 500,
        },
      },
    },
    MuiTab: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          fontWeight: 500,
          minHeight: 48,
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: {
          borderColor: 'rgba(255, 255, 255, 0.06)',
        },
        head: {
          fontWeight: 600,
          backgroundColor: '#232328',
        },
      },
    },
  },
})

export default theme
