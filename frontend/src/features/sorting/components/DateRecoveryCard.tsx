import { Box, Button, Group, NumberInput, Stack, Text } from '@mantine/core';
import { IconAlertTriangle, IconPlayerPlay } from '@tabler/icons-react';
import { useState } from 'react';

interface DateRecoveryCardProps {
  cohortId: number;
  excludedCount: number;
  onRecover: (minYear: number, maxYear: number) => void;
  isLoading?: boolean;
}

export const DateRecoveryCard = ({
  excludedCount,
  onRecover,
  isLoading,
}: DateRecoveryCardProps) => {
  const currentYear = new Date().getFullYear();
  const [minYear, setMinYear] = useState(1980);
  const [maxYear, setMaxYear] = useState(currentYear + 1);

  return (
    <Box
      p="md"
      style={{
        backgroundColor: 'rgba(255, 193, 7, 0.1)',
        border: '2px solid var(--nils-warning)',
        borderRadius: 'var(--nils-radius-md)',
      }}
    >
      <Group gap="sm" mb="md">
        <IconAlertTriangle size={20} color="var(--nils-warning)" />
        <Text fw={600} c="var(--nils-warning)">
          Date Recovery Available
        </Text>
      </Group>
      
      <Text size="sm" c="var(--nils-text-secondary)" mb="md">
        {excludedCount} {excludedCount === 1 ? 'study' : 'studies'} missing dates. Extract from DICOM UIDs?
      </Text>
      
      <Stack gap="md">
        <Group>
          <NumberInput
            label="Min Year"
            value={minYear}
            onChange={(val) => setMinYear(val as number)}
            min={1900}
            max={maxYear}
            style={{ width: 120 }}
            size="sm"
          />
          <NumberInput
            label="Max Year"
            value={maxYear}
            onChange={(val) => setMaxYear(val as number)}
            min={minYear}
            max={2100}
            style={{ width: 120 }}
            size="sm"
          />
        </Group>
        
        <Button
          leftSection={<IconPlayerPlay size={16} />}
          onClick={() => onRecover(minYear, maxYear)}
          loading={isLoading}
          color="orange"
          size="sm"
          fullWidth
        >
          Recover Dates
        </Button>
      </Stack>
    </Box>
  );
};
