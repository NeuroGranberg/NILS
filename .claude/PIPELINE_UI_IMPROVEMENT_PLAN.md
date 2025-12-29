# Pipeline UI Improvement Plan for NILS Neuroimaging Toolkit

## Executive Summary

This plan addresses five key issues in the cohort pipeline UI:
1. Status color duplication across 7 files
2. Inconsistent styling (NILS CSS vars vs Mantine vars)
3. Different form layouts between stage forms
4. Missing reusable components for common patterns
5. Slow page load with multiple API calls

---

## Phase 1: Centralize Status Colors and Constants

### Problem Analysis
Status color configurations are duplicated in:
- `/frontend/src/features/shared/components/PipelineStepper.tsx` (lines 6-14)
- `/frontend/src/features/shared/components/StageCard.tsx` (lines 7-15)
- `/frontend/src/features/cohorts/components/CohortCard.tsx` (lines 19-27)
- `/frontend/src/features/jobs/components/MiniPipelineStepper.tsx` (lines 11-47)
- `/frontend/src/features/jobs/components/JobHistoryTable.tsx` (lines 30-37)
- `/frontend/src/features/jobs/components/SystemJobCard.tsx` (lines 22-29)
- `/frontend/src/features/extraction/ExtractStageForm.tsx` (lines 37-44)

### Solution

**Step 1.1: Create constants file**
Create `/frontend/src/constants/status.ts`:
- Export StageStatus type (from types/stage.ts)
- Export JobStatus type (from types/job.ts)
- Export STAGE_STATUS_CONFIG with color, bgColor, mantineColor, label
- Export JOB_STATUS_CONFIG with color, bgColor, label
- Export helper function getStatusColor(status)
- Export helper function getStatusBgColor(status)

**Step 1.2: Define the status config structure**
```typescript
interface StatusConfig {
  color: string;          // Primary color (CSS variable)
  bgColor: string;        // Background with opacity
  mantineColor: string;   // Mantine color name for components
  label: string;          // Human-readable label
  icon?: ReactNode;       // Optional icon component
}
```

**Step 1.3: Update all consuming files**
Replace local `statusConfig` with import from `/frontend/src/constants/status.ts` in all 7 files listed above.

---

## Phase 2: Create Reusable UI Components

### 2.1: StatusBadge Component
Location: `/frontend/src/features/shared/components/StatusBadge.tsx`

**Purpose**: Unified status indicator used across the application

**Props**:
```typescript
interface StatusBadgeProps {
  status: StageStatus | JobStatus;
  size?: 'xs' | 'sm' | 'md';
  showDot?: boolean;       // Show pulsing dot for running status
  showLabel?: boolean;     // Show text label
  variant?: 'badge' | 'pill' | 'minimal';
}
```

### 2.2: EmptyState Component
Location: `/frontend/src/features/shared/components/EmptyState.tsx`

**Purpose**: Consistent empty state display (CSS class `.nils-empty` exists in styles.css)

**Props**:
```typescript
interface EmptyStateProps {
  icon?: ReactNode;
  title: string;
  description?: string;
  action?: ReactNode;
}
```

### 2.3: SectionCard Component
Location: `/frontend/src/features/shared/components/SectionCard.tsx`

**Purpose**: Consistent card styling for form sections

**Props**:
```typescript
interface SectionCardProps {
  title: string;
  description?: string;
  children: ReactNode;
  collapsible?: boolean;
  defaultExpanded?: boolean;
  headerRight?: ReactNode;
}
```

### 2.4: SelectablePill Component
Location: `/frontend/src/features/shared/components/SelectablePill.tsx`

**Purpose**: Consistent selectable option styling (for modalities, toggles, etc.)

**Props**:
```typescript
interface SelectablePillProps {
  selected: boolean;
  onClick: () => void;
  disabled?: boolean;
  children: ReactNode;
}
```

---

## Phase 3: Standardize Form Layouts

### Solution

**Step 3.1: Create FormSection component**
Location: `/frontend/src/features/shared/components/FormSection.tsx`

**Step 3.2: Create FormFieldGroup component**
For consistent input groups with labels.

**Step 3.3: Define standard spacing tokens**
- Section gap: `gap="lg"` (24px)
- Field gap: `gap="sm"` (8px)
- Indent border: `borderLeft: '1px solid var(--nils-border-subtle)'`

---

## Phase 4: Fix Styling Inconsistencies

### Problem: Mixed CSS Variable Usage
Found instances using `--mantine-color-*` instead of `--nils-*`

### Solution
Replace:
- `--mantine-color-default-border` -> `var(--nils-border)`
- `--mantine-color-body` -> `var(--nils-bg-secondary)`

---

## Phase 5: Optimize API Loading

### Solution

**Step 5.1: Implement query prefetching on card hover**

**Step 5.2: Lazy load non-critical queries**

**Step 5.3: Split CohortDetailPage into sub-components**
- `/frontend/src/features/cohorts/components/CohortHeader.tsx`
- `/frontend/src/features/cohorts/components/CohortStagePanel.tsx`
- `/frontend/src/features/cohorts/components/CohortMetricsCard.tsx`

---

## New Files to Create

```
frontend/src/
├── constants/
│   └── status.ts                    # Centralized status colors
├── features/
│   └── shared/
│       └── components/
│           ├── StatusBadge.tsx      # Status indicator
│           ├── EmptyState.tsx       # Empty state pattern
│           ├── SectionCard.tsx      # Section wrapper
│           ├── SelectablePill.tsx   # Toggle pills
│           ├── FormSection.tsx      # Form section wrapper
│           └── index.ts             # Barrel export
```

## Files to Modify

1. **PipelineStepper.tsx**: Remove local statusConfig, import from constants
2. **StageCard.tsx**: Remove local statusConfig, import from constants
3. **CohortCard.tsx**: Remove local statusConfig, import from constants
4. **MiniPipelineStepper.tsx**: Remove local statusConfig, use StatusBadge
5. **JobHistoryTable.tsx**: Remove local statusConfig, use StatusBadge
6. **SystemJobCard.tsx**: Remove local statusConfig, use StatusBadge
7. **ExtractStageForm.tsx**: Remove local statusBadgeColor, use constants
8. **AnonymizeStageForm.tsx**: Use FormSection, fix CSS variables
9. **SortingPipelineSimple.tsx**: Extract SelectablePill usage

---

## Implementation Order

| Phase | Priority | Dependencies |
|-------|----------|--------------|
| Phase 1 | High | None |
| Phase 2.1-2.2 | High | Phase 1 |
| Phase 4 | Low | None |
| Phase 2.3-2.4 | Medium | Phase 1 |
| Phase 3 | Medium | Phase 2 |
| Phase 5 | Medium | Phase 3 |

**Recommended Order**:
1. Phase 1 (foundations - centralize status)
2. Phase 2.1-2.2 (critical shared components)
3. Phase 4 (quick CSS fixes)
4. Phase 2.3-2.4 (remaining components)
5. Phase 3 (form standardization)
6. Phase 5 (performance optimization)
