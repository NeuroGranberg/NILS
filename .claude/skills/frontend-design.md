---
name: frontend-design
description: Create distinctive, production-grade frontend interfaces for the NILS neuroimaging toolkit. Use this skill when improving UI components, pipeline steps, or cohort-related interfaces.
---

This skill guides creation of cohesive, production-grade frontend interfaces for the NILS (Neuroimaging Sorting Toolkit) application. All implementations must follow the established NILS design system while improving visual consistency and user experience.

## NILS Design System Reference

### Core Color Palette (Dark Theme)
```css
/* Background Layers */
--nils-bg-primary: #0d1117      /* Main surface */
--nils-bg-secondary: #161b22    /* Elevated surfaces */
--nils-bg-tertiary: #21262d     /* Component backgrounds */
--nils-bg-elevated: #30363d     /* Highest elevation */

/* Text Colors */
--nils-text-primary: #f0f6fc    /* Main text */
--nils-text-secondary: #8b949e  /* Secondary text */
--nils-text-tertiary: #6e7681   /* Muted/label text */

/* Accent Colors */
--nils-accent-primary: #58a6ff  /* Primary blue */
--nils-accent-secondary: #388bfd
--nils-accent-hover: #79c0ff

/* Pipeline Stage Status Colors */
--nils-stage-idle: #6e7681      /* Gray */
--nils-stage-pending: #a371f7   /* Purple */
--nils-stage-running: #58a6ff   /* Blue with pulse */
--nils-stage-completed: #3fb950 /* Green */
--nils-stage-failed: #f85149    /* Red */
--nils-stage-paused: #d29922    /* Yellow */
--nils-stage-blocked: #484f58   /* Dark gray */

/* Semantic Colors */
--nils-success: #3fb950
--nils-warning: #d29922
--nils-error: #f85149
--nils-info: #58a6ff
```

### Spacing & Layout
```css
--nils-space-xs: 4px
--nils-space-sm: 8px
--nils-space-md: 16px
--nils-space-lg: 24px
--nils-space-xl: 32px
--nils-space-2xl: 48px

--nils-radius-sm: 4px
--nils-radius-md: 8px
--nils-radius-lg: 12px
```

### Typography
- Font Family: Inter with system-ui fallback
- Use Mantine's Text component with `size`, `fw`, `c` props
- Hierarchy: Title (order 1-4), Text (lg/md/sm/xs)

## Component Implementation Guidelines

### Pipeline Step Components
When creating or improving pipeline step UI:

1. **Consistent Card Structure**
   - Use `Paper` with `bg="var(--nils-bg-secondary)"`
   - Border: `1px solid var(--nils-border-subtle)`
   - Padding: `md` (16px) consistently
   - Border radius: `md` (8px)

2. **Step Status Indicators**
   - Always use centralized status config (create if not exists)
   - Animated pulse for "running" status
   - Consistent icon sizing (16-20px)

3. **Form Elements**
   - Input backgrounds: `var(--nils-bg-primary)`
   - Focus ring: `var(--nils-accent-primary)`
   - Labels: `var(--nils-text-secondary)`, size "sm"
   - Group related inputs with consistent gap="md"

4. **Selectable Items (Pills, Chips, Options)**
   - Inactive: `bg: var(--nils-bg-tertiary)`, `color: var(--nils-text-secondary)`
   - Active: `bg: var(--nils-accent-primary)`, `color: var(--nils-bg-primary)`
   - Hover: Subtle border or background transition
   - Consistent height (32-36px for standard, 28px for compact)

5. **Action Buttons**
   - Primary: Blue accent, used for main actions (Run, Save)
   - Secondary: Tertiary background, for cancel/back
   - Danger: Red for destructive actions
   - Icon buttons: Ghost variant, consistent sizing

### Empty States
Create reusable `EmptyState` component:
- Centered layout with icon (48-64px, muted color)
- Title in `--nils-text-primary`
- Description in `--nils-text-secondary`
- Optional action button

### Loading States
- Use Mantine `Loader` with color="nils"
- Skeleton placeholders for content areas
- Shimmer animation for loading cards

### Animations & Transitions
```css
--nils-transition-fast: 150ms ease
--nils-transition-normal: 250ms ease
```
- Apply to hover states, focus rings, color changes
- Pulse animation for running indicators
- Staggered reveal for step lists

## Key Files Reference

| Component | Path |
|-----------|------|
| Main Pipeline Page | `frontend/src/features/cohorts/pages/CohortDetailPage.tsx` |
| Pipeline Stepper | `frontend/src/features/shared/components/PipelineStepper.tsx` |
| Stage Card | `frontend/src/features/shared/components/StageCard.tsx` |
| Sorting Pipeline | `frontend/src/features/sorting/components/SortingPipelineSimple.tsx` |
| Anonymization Form | `frontend/src/features/anonymization/AnonymizeStageForm.tsx` |
| Extraction Form | `frontend/src/features/extraction/ExtractStageForm.tsx` |
| Global Styles | `frontend/src/app/styles.css` |
| Theme Config | `frontend/src/app/providers/AppProviders.tsx` |

## Implementation Priorities

1. **Centralize Status Configurations** - Create shared status config to eliminate duplication
2. **Standardize Form Layouts** - Consistent spacing, labels, input styling
3. **Unify Selectable Elements** - Pills, chips, toggle buttons with same visual treatment
4. **Improve Step Transitions** - Smooth animations between pipeline steps
5. **Create Shared Components** - EmptyState, LoadingCard, StatusBadge
