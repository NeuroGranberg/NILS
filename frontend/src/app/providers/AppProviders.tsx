import { MantineProvider, createTheme } from '@mantine/core';
import { Notifications } from '@mantine/notifications';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { type ReactNode } from 'react';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 1000 * 5,
      gcTime: 1000 * 60 * 5, // Garbage collect unused queries after 5 minutes
    },
  },
});

// NILS Design System Theme
const nilsTheme = createTheme({
  // Typography
  fontFamily: 'Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  headings: {
    fontFamily: 'Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    fontWeight: '600',
  },
  
  // Border radius - consistent and subtle
  radius: {
    xs: '4px',
    sm: '6px',
    md: '8px',
    lg: '12px',
    xl: '16px',
  },
  defaultRadius: 'md',
  
  // Spacing scale
  spacing: {
    xs: '4px',
    sm: '8px',
    md: '16px',
    lg: '24px',
    xl: '32px',
  },
  
  // Custom colors aligned with NILS palette
  colors: {
    // Primary blue - medical/scientific feel
    nils: [
      '#e6f2ff',
      '#cce5ff',
      '#99cbff',
      '#66b1ff',
      '#3397ff',
      '#0a84ff',
      '#006ee6',
      '#0058cc',
      '#0042b3',
      '#002c99',
    ],
    // Neutral grays
    dark: [
      '#f0f6fc',
      '#c9d1d9',
      '#b1bac4',
      '#8b949e',
      '#6e7681',
      '#484f58',
      '#30363d',
      '#21262d',
      '#161b22',
      '#0d1117',
    ],
  },
  primaryColor: 'nils',
  primaryShade: 5,
  
  // Component-specific overrides for clean, flat design
  components: {
    Button: {
      defaultProps: {
        radius: 'md',
      },
      styles: {
        root: {
          fontWeight: 600,
          transition: 'all 150ms ease',
        },
      },
    },
    Card: {
      defaultProps: {
        radius: 'lg',
        padding: 'lg',
      },
      styles: {
        root: {
          backgroundColor: 'var(--nils-bg-secondary)',
          border: '1px solid var(--nils-border-subtle)',
        },
      },
    },
    Paper: {
      defaultProps: {
        radius: 'md',
      },
      styles: {
        root: {
          backgroundColor: 'var(--nils-bg-secondary)',
        },
      },
    },
    TextInput: {
      defaultProps: {
        radius: 'md',
      },
      styles: {
        input: {
          backgroundColor: 'var(--nils-bg-primary)',
          borderColor: 'var(--nils-border)',
          '&:focus': {
            borderColor: 'var(--nils-accent-primary)',
          },
        },
      },
    },
    NumberInput: {
      defaultProps: {
        radius: 'md',
      },
      styles: {
        input: {
          backgroundColor: 'var(--nils-bg-primary)',
          borderColor: 'var(--nils-border)',
        },
      },
    },
    Select: {
      defaultProps: {
        radius: 'md',
      },
      styles: {
        input: {
          backgroundColor: 'var(--nils-bg-primary)',
          borderColor: 'var(--nils-border)',
        },
      },
    },
    Textarea: {
      defaultProps: {
        radius: 'md',
      },
      styles: {
        input: {
          backgroundColor: 'var(--nils-bg-primary)',
          borderColor: 'var(--nils-border)',
        },
      },
    },
    Modal: {
      defaultProps: {
        radius: 'lg',
        centered: true,
      },
      styles: {
        content: {
          backgroundColor: 'var(--nils-bg-secondary)',
        },
        header: {
          backgroundColor: 'var(--nils-bg-secondary)',
        },
      },
    },
    Badge: {
      defaultProps: {
        radius: 'sm',
      },
      styles: {
        root: {
          fontWeight: 600,
          textTransform: 'uppercase',
          letterSpacing: '0.03em',
          fontSize: '11px',
        },
      },
    },
    Table: {
      styles: {
        th: {
          fontWeight: 600,
          textTransform: 'uppercase',
          fontSize: '11px',
          letterSpacing: '0.05em',
          color: 'var(--nils-text-tertiary)',
        },
      },
    },
    NavLink: {
      defaultProps: {
        radius: 'md',
      },
      styles: {
        root: {
          transition: 'all 150ms ease',
        },
      },
    },
    Progress: {
      defaultProps: {
        radius: 'sm',
      },
      styles: {
        root: {
          backgroundColor: 'var(--nils-bg-tertiary)',
        },
      },
    },
    Tabs: {
      styles: {
        tab: {
          fontWeight: 500,
          transition: 'all 150ms ease',
        },
      },
    },
    Stepper: {
      styles: {
        stepLabel: {
          fontWeight: 500,
        },
      },
    },
    Notification: {
      defaultProps: {
        radius: 'md',
      },
    },
  },
});

interface AppProvidersProps {
  children: ReactNode;
}

export const AppProviders = ({ children }: AppProvidersProps) => {
  return (
    <QueryClientProvider client={queryClient}>
      <MantineProvider theme={nilsTheme} defaultColorScheme="dark">
        <Notifications position="top-right" />
        {children}
      </MantineProvider>
    </QueryClientProvider>
  );
};
