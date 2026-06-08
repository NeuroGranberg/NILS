import { Radio, Stack, Text } from '@mantine/core';
import { useEffect, useMemo, useRef } from 'react';
import { SectionCard } from '../../shared/components/SectionCard';
import { SchemaField } from './SchemaField';
import type {
  DescriptorGroup,
  DescriptorInput,
  FormValue,
  FormValues,
} from '../types';

interface SchemaFormProps {
  inputs: DescriptorInput[];
  groups?: DescriptorGroup[];
  /** Controlled values map: { [input.id]: value } */
  value: FormValues;
  onChange: (next: FormValues) => void;
  /** Optional: notified whenever the computed validity changes (gates Launch). */
  onValidityChange?: (valid: boolean) => void;
}

const isRequired = (input: DescriptorInput): boolean => input.optional === false;

// NILS supplies these reserved BIDS-Apps args itself — the input paths via the
// input selection, the fan-out label via the work_unit, and the output path via
// RunControls. They must NOT be rendered as user knobs or posted into params, or
// they would duplicate/override what NILS injects at run time.
const RESERVED_INPUT_KEYS = new Set([
  'InputDataset',
  'OutputLocation',
  'SubjectLabel',
  'ParticipantLabel',
  'SessionLabel',
]);
const stripKey = (s: string | null | undefined): string =>
  (s ?? '').replace(/[[\]{}]/g, '').trim();
const isReservedInput = (input: DescriptorInput): boolean =>
  RESERVED_INPUT_KEYS.has(input.id) || RESERVED_INPUT_KEYS.has(stripKey(input['value-key']));

const isEmptyValue = (v: FormValue | undefined): boolean => {
  if (v === undefined || v === null) return true;
  if (typeof v === 'string') return v.trim() === '';
  if (Array.isArray(v)) return v.length === 0;
  return false;
};

const defaultFor = (input: DescriptorInput): FormValue | undefined => {
  const dv = input['default-value'];
  if (dv === undefined) {
    // Flags without an explicit default seed to false so the control is defined.
    return input.type === 'Flag' ? false : undefined;
  }
  if (typeof dv === 'boolean' || typeof dv === 'string' || typeof dv === 'number') {
    return dv;
  }
  if (Array.isArray(dv)) {
    return dv.map((x) => (typeof x === 'number' ? x : String(x))) as
      | string[]
      | number[];
  }
  return undefined;
};

/**
 * SchemaForm - the schema-driven config renderer (§15.2). A pure function of the
 * descriptor's `inputs` (+ optional `groups`). It renders one SchemaField per
 * input, groups mutually-exclusive members into a single "pick one" control,
 * seeds missing keys from `default-value`, computes required/group validity, and
 * never drops a key it does not fully understand.
 *
 * Yields a `{ [input.id]: value }` params object via `onChange`.
 */
export const SchemaForm = ({
  inputs,
  groups,
  value,
  onChange,
  onValidityChange,
}: SchemaFormProps) => {
  // Reserved/fan-out args are NILS-supplied, not user knobs — exclude them from
  // rendering, seeding, and validity (and therefore from the posted params).
  const renderedInputs = useMemo(
    () => inputs.filter((inp) => !isReservedInput(inp)),
    [inputs],
  );

  const inputsById = useMemo(() => {
    const m = new Map<string, DescriptorInput>();
    for (const inp of renderedInputs) m.set(inp.id, inp);
    return m;
  }, [renderedInputs]);

  // Members that belong to a mutually-exclusive group are rendered inside the
  // group block, not as standalone fields.
  const mutexGroups = useMemo(
    () => (groups ?? []).filter((g) => g['mutually-exclusive']),
    [groups],
  );
  const mutexMemberIds = useMemo(() => {
    const s = new Set<string>();
    for (const g of mutexGroups) for (const id of g.members) s.add(id);
    return s;
  }, [mutexGroups]);

  const standaloneInputs = useMemo(
    () => renderedInputs.filter((inp) => !mutexMemberIds.has(inp.id)),
    [renderedInputs, mutexMemberIds],
  );

  // ---- Seed missing keys from default-value (only fill absent keys) ----
  // Track which input-set we've already seeded for, to avoid clobbering edits.
  const seededRef = useRef<string>('');
  useEffect(() => {
    const sig = renderedInputs.map((i) => i.id).join('|');
    const patch: FormValues = {};
    let changed = false;
    for (const inp of renderedInputs) {
      if (!(inp.id in value)) {
        const dv = defaultFor(inp);
        if (dv !== undefined) {
          patch[inp.id] = dv;
          changed = true;
        }
      }
    }
    if (changed) {
      onChange({ ...value, ...patch });
    }
    seededRef.current = sig;
    // We intentionally depend on `renderedInputs` only; re-seeding on every
    // `value` change would fight user edits. The `in value` guard keeps edits.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [renderedInputs]);

  // ---- Validity computation ----
  const valid = useMemo(() => {
    // Required standalone inputs must be non-empty.
    for (const inp of renderedInputs) {
      if (mutexMemberIds.has(inp.id)) continue;
      if (isRequired(inp) && isEmptyValue(value[inp.id])) return false;
    }
    // Group constraints.
    for (const g of groups ?? []) {
      const members = g.members.filter((id) => inputsById.has(id));
      const setMembers = members.filter((id) => !isEmptyValue(value[id]));
      if (g['mutually-exclusive'] && setMembers.length > 1) return false;
      if (g['one-is-required'] && setMembers.length === 0) return false;
      if (g['all-or-none'] && setMembers.length > 0 && setMembers.length !== members.length) {
        return false;
      }
    }
    return true;
  }, [renderedInputs, groups, value, mutexMemberIds, inputsById]);

  useEffect(() => {
    onValidityChange?.(valid);
  }, [valid, onValidityChange]);

  const setField = (id: string, v: FormValue) => {
    onChange({ ...value, [id]: v });
  };

  // For a mutex group, the selected member id is the radio choice. Selecting a
  // member sets it (to a truthy placeholder if it has no value yet) and clears
  // the others. A member that is itself a value-bearing input still renders its
  // own control once selected.
  const selectedMemberOf = (g: DescriptorGroup): string => {
    for (const id of g.members) {
      if (!isEmptyValue(value[id])) return id;
    }
    return '';
  };

  const handleMutexSelect = (g: DescriptorGroup, memberId: string) => {
    const patch: FormValues = { ...value };
    for (const id of g.members) {
      if (id === memberId) {
        const inp = inputsById.get(id);
        // If it has no value yet, seed from default or a truthy placeholder so
        // the selection is recorded; value-bearing fields then refine it.
        if (isEmptyValue(patch[id])) {
          const dv = inp ? defaultFor(inp) : undefined;
          patch[id] = dv !== undefined && dv !== false ? dv : true;
        }
      } else {
        patch[id] = null;
      }
    }
    onChange(patch);
  };

  return (
    <Stack gap="md">
      {standaloneInputs.map((inp) => (
        <SchemaField
          key={inp.id}
          input={inp}
          value={value[inp.id] ?? null}
          onChange={(v) => setField(inp.id, v)}
        />
      ))}

      {mutexGroups.map((g) => {
        const selected = selectedMemberOf(g);
        const groupRequired = g['one-is-required'] === true;
        return (
          <SectionCard
            key={g.id}
            title={g.name ?? g.id}
            description={groupRequired ? 'Select one option (required)' : 'Select one option'}
          >
            <Radio.Group
              value={selected}
              onChange={(memberId) => handleMutexSelect(g, memberId)}
              required={groupRequired}
              withAsterisk={groupRequired}
            >
              <Stack gap="sm">
                {g.members.map((id) => {
                  const inp = inputsById.get(id);
                  if (!inp) return null;
                  const isSelected = selected === id;
                  return (
                    <Stack key={id} gap="xs">
                      <Radio value={id} label={inp.name} />
                      {isSelected && inp.type !== 'Flag' && (
                        <SchemaField
                          input={inp}
                          value={value[id] ?? null}
                          onChange={(v) => setField(id, v)}
                        />
                      )}
                    </Stack>
                  );
                })}
              </Stack>
            </Radio.Group>
          </SectionCard>
        );
      })}

      {standaloneInputs.length === 0 && mutexGroups.length === 0 && (
        <Text size="sm" c="dimmed">
          This pipeline has no configurable parameters.
        </Text>
      )}
    </Stack>
  );
};
