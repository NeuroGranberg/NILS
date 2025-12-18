import { Anchor, Box, Center, Stack, Text, Title } from '@mantine/core';
import { IconAlertTriangle } from '@tabler/icons-react';
import { Link } from 'react-router-dom';

export const NotFoundPage = () => (
  <Center h="100%">
    <Stack gap="md" align="center">
      <Box
        style={{
          width: 64,
          height: 64,
          borderRadius: 'var(--nils-radius-lg)',
          backgroundColor: 'var(--nils-bg-tertiary)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        <IconAlertTriangle size={32} color="var(--nils-text-tertiary)" />
      </Box>
      <Stack gap={4} align="center">
        <Title order={2} fw={600} c="var(--nils-text-primary)">
          Page not found
        </Title>
        <Text c="var(--nils-text-secondary)" size="sm">
          The page you're looking for doesn't exist in NILS.
        </Text>
      </Stack>
      <Anchor component={Link} to="/cohorts" c="var(--nils-accent-primary)" fw={500}>
        Return to Cohorts
      </Anchor>
    </Stack>
  </Center>
);
