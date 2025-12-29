---
name: pipeline-steps
description: Improve and standardize cohort pipeline step components. Use this skill when working on pipeline step UI, step forms, or step navigation.
---

This skill focuses on improving the cohort pipeline step components for visual consistency and better user experience.

## Current Pipeline Steps

The pipeline has 4 main stages, each with their own configuration UI:

1. **Anonymize** - Patient ID mapping, DICOM anonymization settings
2. **Extract** - Performance tuning, worker configuration
3. **Sort** - 5-step classification process with real-time progress
4. **BIDS** - Export format selection, layout options

## Improvement Priorities

### 1. Consistent Step Header Pattern
Each step should have:
- Step number badge (circular, accent color)
- Step title (Title order={4}, primary text)
- Step description (Text size="sm", secondary text)
- Optional status badge aligned right

### 2. Unified Option Groups
Configuration options should use consistent patterns:

**Toggle Options (Boolean)**
```tsx
<Switch
  label="Option Name"
  description="What this option does"
  size="md"
/>
```

**Select Options (Single Choice)**
```tsx
<Select
  label="Option Name"
  description="What this selects"
  data={options}
  styles={{ input: { backgroundColor: 'var(--nils-bg-primary)' } }}
/>
```

**Multi-Select Options (Pills/Chips)**
```tsx
<Group gap="xs">
  {options.map(opt => (
    <Button
      key={opt.value}
      variant={selected.includes(opt.value) ? 'filled' : 'light'}
      color={selected.includes(opt.value) ? 'nils' : 'gray'}
      size="compact-sm"
      onClick={() => toggle(opt.value)}
    >
      {opt.label}
    </Button>
  ))}
</Group>
```

### 3. Section Containers
Group related options in sections:
```tsx
<Paper p="md" bg="var(--nils-bg-secondary)" radius="md">
  <Stack gap="md">
    <Text fw={600} size="sm" c="var(--nils-text-secondary)">
      Section Title
    </Text>
    {/* Options go here */}
  </Stack>
</Paper>
```

### 4. Number Inputs with Context
For worker counts, batch sizes, etc:
```tsx
<NumberInput
  label="Worker Count"
  description="Recommended: {sysRecommendation}"
  min={1}
  max={maxCores}
  rightSection={<Text size="xs" c="dimmed">/ {maxCores}</Text>}
/>
```

### 5. Collapsible Advanced Options
Keep the UI clean by hiding advanced options:
```tsx
<Collapse.Root>
  <Collapse.Trigger>
    <Button variant="subtle" leftSection={<ChevronDown />}>
      Advanced Options
    </Button>
  </Collapse.Trigger>
  <Collapse.Content>
    {/* Advanced options */}
  </Collapse.Content>
</Collapse.Root>
```

## Key Components to Improve

### PipelineStepper.tsx
- Add smooth transition animations between steps
- Improve status indicator visibility
- Add completion percentage where applicable

### StageCard.tsx
- Standardize padding and spacing
- Improve action button layout
- Add subtle entry animation

### Stage Forms (Anonymize, Extract, Sort, BIDS)
- Apply consistent section grouping
- Standardize input styling
- Add helpful tooltips/descriptions
- Use consistent spacing (gap="md" between sections, gap="sm" within)

## Status Color Constants

Create centralized constants to eliminate duplication:

```typescript
// frontend/src/constants/statusColors.ts
export const STAGE_STATUS_COLORS = {
  idle: { color: 'gray', bg: 'var(--nils-stage-idle)' },
  pending: { color: 'violet', bg: 'var(--nils-stage-pending)' },
  running: { color: 'blue', bg: 'var(--nils-stage-running)' },
  completed: { color: 'teal', bg: 'var(--nils-stage-completed)' },
  failed: { color: 'red', bg: 'var(--nils-stage-failed)' },
  paused: { color: 'yellow', bg: 'var(--nils-stage-paused)' },
  blocked: { color: 'gray', bg: 'var(--nils-stage-blocked)' },
} as const;
```
