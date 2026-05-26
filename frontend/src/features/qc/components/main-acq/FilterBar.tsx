/**
 * Filter bar for the Main Acquisition QC page.
 *
 * UX:
 * - One fixed-width dropdown-button per facet, opening a popover with a
 *   searchable checkbox list (tick to select multiple values; each facet is
 *   OR internally, AND between facets).
 * - A draft copy of the filter state lives here; the parent only sees updates
 *   when the user hits "Apply". This avoids hammering the backend on every
 *   keystroke or toggle.
 * - Selected values show up as chips in a row BELOW the dropdowns, grouped
 *   by facet, so the selection is visible even when the popover is closed.
 * - Options are computed from the APPLIED filters (cascading), not from the
 *   draft, so ticking something doesn't shrink the dropdown mid-selection.
 */

import { useEffect, useMemo, useState } from 'react';
import {
  Box,
  Button,
  Checkbox,
  Group,
  Popover,
  ScrollArea,
  Stack,
  Text,
  TextInput,
  Badge,
  Divider,
} from '@mantine/core';
import { IconChevronDown, IconX } from '@tabler/icons-react';
import type { Facet, FilterState } from '../../api/main-acq';
import { FACETS, NONE_SENTINEL, useMainAcqFilterOptions } from '../../api/main-acq';

const FACET_LABEL: Record<Facet, string> = {
  directory_type: 'Intent',
  provenance: 'Provenance',
  orientation: 'Orientation',
  base: 'Base',
  mr_acquisition_type: 'Acq type',
  technique: 'Technique',
  modifier_csv: 'Modifier',
};

const FACET_BUTTON_WIDTH = 150;

interface FilterBarProps {
  cohortId: number;
  appliedFilters: FilterState;
  onApply: (next: FilterState) => void;
  includeAcceleration: boolean;
  onIncludeAccelerationChange: (value: boolean) => void;
  onlyMultiStack: boolean;
  onOnlyMultiStackChange: (value: boolean) => void;
}

const emptyFilters = (): FilterState => {
  const out: FilterState = {};
  FACETS.forEach((f) => {
    out[f] = null;
  });
  return out;
};

const filtersEqual = (a: FilterState, b: FilterState) => {
  for (const f of FACETS) {
    const av = a[f] ?? [];
    const bv = b[f] ?? [];
    if (av.length !== bv.length) return false;
    const aa = [...av].sort();
    const bb = [...bv].sort();
    for (let i = 0; i < aa.length; i++) if (aa[i] !== bb[i]) return false;
  }
  return true;
};

export const FilterBar = ({ cohortId, appliedFilters, onApply, includeAcceleration, onIncludeAccelerationChange, onlyMultiStack, onOnlyMultiStackChange }: FilterBarProps) => {
  // Options are driven by the LAST APPLIED filter state only, so the popover
  // doesn't shuffle as the user is still ticking boxes.
  const { data: optionsData } = useMainAcqFilterOptions(cohortId, appliedFilters);

  const [draft, setDraft] = useState<FilterState>(appliedFilters);

  // Keep the draft in sync when Apply lands (or Reset is clicked externally).
  useEffect(() => {
    setDraft(appliedFilters);
  }, [appliedFilters]);

  const dirty = !filtersEqual(draft, appliedFilters);

  const toggleValue = (facet: Facet, value: string) => {
    const current = draft[facet] ?? [];
    const next = current.includes(value)
      ? current.filter((v) => v !== value)
      : [...current, value];
    setDraft({ ...draft, [facet]: next.length > 0 ? next : null });
  };

  const clearFacet = (facet: Facet) => {
    setDraft({ ...draft, [facet]: null });
  };

  const handleReset = () => {
    const empty = emptyFilters();
    setDraft(empty);
    onApply(empty);
  };

  const handleApply = () => {
    onApply(draft);
  };

  const anySelected = useMemo(() => {
    return FACETS.some((f) => (draft[f]?.length ?? 0) > 0);
  }, [draft]);

  return (
    <Stack gap="xs">
      <Group gap="xs" wrap="wrap" align="center">
        {FACETS.map((facet) => (
          <FacetDropdown
            key={facet}
            label={FACET_LABEL[facet]}
            selected={draft[facet] ?? []}
            options={optionsData?.options?.[facet] ?? []}
            onToggle={(v) => toggleValue(facet, v)}
            onClear={() => clearFacet(facet)}
          />
        ))}
        <Box style={{ flex: 1 }} />
        <Button
          size="xs"
          onClick={handleApply}
          disabled={!dirty}
          variant={dirty ? 'filled' : 'light'}
        >
          Apply
        </Button>
        <Button size="xs" variant="subtle" onClick={handleReset} disabled={!anySelected && !dirty}>
          Reset
        </Button>
      </Group>

      <Divider variant="dashed" />
      <Group gap={6} wrap="wrap" justify="space-between">
        <Group gap={6} wrap="wrap" style={{ flex: 1 }}>
          {FACETS.filter((f) => (draft[f]?.length ?? 0) > 0).map((facet) => (
            <Group key={facet} gap={4} wrap="nowrap">
              <Text size="xs" c="dimmed" fw={600}>
                {FACET_LABEL[facet]}:
              </Text>
              {(draft[facet] ?? []).map((v) => (
                <Badge
                  key={v}
                  size="sm"
                  color="blue"
                  variant="light"
                  rightSection={
                    <IconX
                      size={11}
                      style={{ cursor: 'pointer' }}
                      onClick={() => toggleValue(facet, v)}
                    />
                  }
                >
                  {v === NONE_SENTINEL ? '(none)' : v}
                </Badge>
              ))}
            </Group>
          ))}
        </Group>
        <Checkbox
          size="xs"
          label="Include acceleration in name"
          checked={includeAcceleration}
          onChange={(e) => onIncludeAccelerationChange(e.currentTarget.checked)}
        />
        <Checkbox
          size="xs"
          label="Only multi-stack bundles"
          checked={onlyMultiStack}
          onChange={(e) => onOnlyMultiStackChange(e.currentTarget.checked)}
        />
      </Group>
    </Stack>
  );
};

// --------------------------------------------------------------------------- //
// FacetDropdown
// --------------------------------------------------------------------------- //

interface FacetDropdownProps {
  label: string;
  selected: string[];
  options: string[];
  onToggle: (value: string) => void;
  onClear: () => void;
}

const FacetDropdown = ({ label, selected, options, onToggle, onClear }: FacetDropdownProps) => {
  const [opened, setOpened] = useState(false);
  const [search, setSearch] = useState('');

  const filtered = useMemo(() => {
    const s = search.trim().toLowerCase();
    if (!s) return options;
    return options.filter((v) => v.toLowerCase().includes(s));
  }, [options, search]);

  const buttonLabel =
    selected.length === 0
      ? 'Any'
      : selected.length === 1
        ? selected[0] === NONE_SENTINEL
          ? '(none)'
          : selected[0]
        : `${selected.length} selected`;

  return (
    <Stack gap={2}>
      <Text size="xs" fw={600} c="dimmed">
        {label}
      </Text>
      <Popover
        opened={opened}
        onChange={setOpened}
        position="bottom-start"
        withinPortal
        shadow="md"
        trapFocus
      >
        <Popover.Target>
          <Button
            size="xs"
            variant={selected.length > 0 ? 'light' : 'default'}
            color={selected.length > 0 ? 'blue' : 'gray'}
            rightSection={<IconChevronDown size={12} />}
            onClick={() => setOpened((o) => !o)}
            styles={{
              root: { width: FACET_BUTTON_WIDTH, justifyContent: 'space-between' },
              label: { overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
            }}
          >
            {buttonLabel}
          </Button>
        </Popover.Target>
        <Popover.Dropdown p="xs">
          <Stack gap="xs">
            <TextInput
              size="xs"
              placeholder="Search…"
              value={search}
              onChange={(e) => setSearch(e.currentTarget.value)}
            />
            <ScrollArea.Autosize mah={240}>
              <Stack gap={4}>
                {filtered.length === 0 && (
                  <Text size="xs" c="dimmed" ta="center">
                    No options
                  </Text>
                )}
                {filtered.map((v) => (
                  <Checkbox
                    key={v}
                    size="xs"
                    label={v === NONE_SENTINEL ? <i>(none)</i> : v}
                    checked={selected.includes(v)}
                    onChange={() => onToggle(v)}
                  />
                ))}
              </Stack>
            </ScrollArea.Autosize>
            <Group justify="space-between">
              <Button
                size="compact-xs"
                variant="subtle"
                onClick={onClear}
                disabled={selected.length === 0}
              >
                Clear
              </Button>
              <Button size="compact-xs" variant="subtle" onClick={() => setOpened(false)}>
                Close
              </Button>
            </Group>
          </Stack>
        </Popover.Dropdown>
      </Popover>
    </Stack>
  );
};
