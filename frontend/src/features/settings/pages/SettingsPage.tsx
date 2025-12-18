import { Badge, Box, Card, Group, List, Stack, Text, Title } from '@mantine/core';
import { IconBrain, IconCpu } from '@tabler/icons-react';

export const SettingsPage = () => (
  <Stack gap="lg" p="md">
    <Stack gap={4}>
      <Title order={2} fw={600} c="var(--nils-text-primary)">
        Settings
      </Title>
      <Text size="sm" c="var(--nils-text-secondary)">
        LLM configuration placeholders – functionality coming soon
      </Text>
    </Stack>

    <Card
      padding="lg"
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        border: '1px solid var(--nils-border-subtle)',
        borderRadius: 'var(--nils-radius-lg)',
      }}
    >
      <Stack gap="md">
        <Group gap="sm">
          <Box
            style={{
              width: 32,
              height: 32,
              borderRadius: 'var(--nils-radius-sm)',
              backgroundColor: 'var(--nils-bg-tertiary)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <IconBrain size={18} color="var(--nils-accent-primary)" />
          </Box>
          <Stack gap={2}>
            <Group gap="xs">
              <Text fw={600} size="sm" c="var(--nils-text-primary)">
                Cloud LLM providers
              </Text>
              <Badge variant="light" color="gray" size="sm">
                Coming soon
              </Badge>
            </Group>
            <Text size="xs" c="var(--nils-text-tertiary)">
              Connect hosted providers for assisted labeling and insights.
            </Text>
          </Stack>
        </Group>
        <List spacing="xs" size="sm" c="var(--nils-text-secondary)">
          <List.Item>Log in to OpenAI</List.Item>
          <List.Item>Log in to Anthropic</List.Item>
          <List.Item>Log in to Google</List.Item>
        </List>
      </Stack>
    </Card>

    <Card
      padding="lg"
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        border: '1px solid var(--nils-border-subtle)',
        borderRadius: 'var(--nils-radius-lg)',
      }}
    >
      <Stack gap="md">
        <Group gap="sm">
          <Box
            style={{
              width: 32,
              height: 32,
              borderRadius: 'var(--nils-radius-sm)',
              backgroundColor: 'var(--nils-bg-tertiary)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <IconCpu size={18} color="var(--nils-accent-primary)" />
          </Box>
          <Stack gap={2}>
            <Group gap="xs">
              <Text fw={600} size="sm" c="var(--nils-text-primary)">
                Local / on-prem LLMs
              </Text>
              <Badge variant="light" color="gray" size="sm">
                Coming soon
              </Badge>
            </Group>
            <Text size="xs" c="var(--nils-text-tertiary)">
              Run models on your own hardware with GPU acceleration.
            </Text>
          </Stack>
        </Group>
        <List spacing="xs" size="sm" c="var(--nils-text-secondary)">
          <List.Item>Use local models via Ollama</List.Item>
          <List.Item>Serve models with vLLM</List.Item>
          <List.Item>Leverage server GPU resources</List.Item>
        </List>
      </Stack>
    </Card>
  </Stack>
);
