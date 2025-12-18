import { useState } from 'react';
import { Box, Group, Stack, Text, SimpleGrid, Card, Badge, Modal, Loader, TextInput, Pagination, Center, Select, Tooltip, Breadcrumbs, Anchor, ThemeIcon, Paper, UnstyledButton, Collapse, Image } from '@mantine/core';
import { useLocalStorage } from '@mantine/hooks';
import { IconCalendar, IconFolder, IconSearch, IconUsers, IconCalendarStats, IconFolders, IconChevronDown, IconTarget } from '@tabler/icons-react';
import { useQuery, keepPreviousData } from '@tanstack/react-query';
import { apiClient } from '../../../utils/api-client';
import { DicomViewer } from '../components/DicomViewer';
import { SingleStackQCModal } from '../components/SingleStackQCModal';
import type { Cohort } from '../../../types';
import type { AxisFlagType } from '../types';

interface QCViewerPageProps {
    cohort: Cohort;
    onBack: () => void;
}

// --- Interfaces ---
interface SubjectIdentifier {
    type: string;
    value: string;
}

interface Subject {
    id: number;
    code: string;
    sex: string | null;
    birth_date: string | null;
    created_at: string;
    other_ids: SubjectIdentifier[];
}

interface Session {
    date: string;
    study_count: number;
    modalities: string[];
}

interface StackItem {
    series_stack_id: number;
    series_instance_uid: string;
    study_date: string | null;
    series_time: string | null;  // For sorting by acquisition time
    modality: string;
    series_description: string | null;
    slices_count: number;
    stack_index: number;
    // Classification fields
    directory_type: string | null;
    base: string | null;
    technique: string | null;
    modifier_csv: string | null;
    construct_csv: string | null;
    provenance: string | null;
    acceleration_csv: string | null;
    post_contrast: number | null;  // 0=pre, 1=post, null=unknown
    spinal_cord: number | null;    // 0=brain, 1=spine, null=unknown
    orientation: string | null;    // Axial/Coronal/Sagittal
    // QC Review fields
    manual_review_required: number | null;
    manual_review_reasons_csv: string | null;
}

// --- Helper Functions ---

// Orientation abbreviation mapping
const ORIENT_ABBREV: Record<string, string> = {
    'Axial': 'Ax',
    'Coronal': 'Cor',
    'Sagittal': 'Sag',
};

// Build classification-based name: Ax_T1w_MPRAGE_SWI or Sag_T2w-fat-sat_TSE_GRAPPA_MIP
const buildStackName = (stack: StackItem): string => {
    const orient = stack.orientation ? (ORIENT_ABBREV[stack.orientation] || stack.orientation.substring(0, 3)) : '';
    const base = stack.base || '';
    const mods = stack.modifier_csv?.replace(/,/g, '-') || '';
    const tech = stack.technique || '';
    const accel = stack.acceleration_csv?.replace(/,/g, '-') || '';
    const construct = stack.construct_csv?.replace(/,/g, '-') || '';
    
    const parts = [orient, base, mods, tech, accel, construct].filter(Boolean);
    return parts.join('_') || stack.series_description || 'Unknown';
};

// Group stacks by directory_type
const groupStacksByIntent = (stacks: StackItem[]): Record<string, StackItem[]> => {
    const groups: Record<string, StackItem[]> = {};
    const order = ['anat', 'dwi', 'func', 'fmap', 'perf', 'localizer', 'misc', 'excluded'];
    
    // Initialize in order
    order.forEach(intent => { groups[intent] = []; });
    
    stacks.forEach(stack => {
        const intent = stack.directory_type || 'misc';
        if (!groups[intent]) groups[intent] = [];
        groups[intent].push(stack);
    });
    
    // Remove empty groups
    Object.keys(groups).forEach(k => {
        if (groups[k].length === 0) delete groups[k];
    });
    
    return groups;
};

// Status badges for contrast and spinal cord - subtle styling for dark theme
const StackBadges = ({ postContrast, spinalCord }: { postContrast: number | null; spinalCord: number | null }) => {
    return (
        <Group gap={4}>
            {postContrast === 1 && (
                <Badge size="xs" color="violet" variant="light" title="Post-contrast (Gadolinium)">
                    CE
                </Badge>
            )}
            {spinalCord === 1 && (
                <Badge size="xs" color="teal" variant="light" title="Spinal Cord">
                    SC
                </Badge>
            )}
        </Group>
    );
};

// Provenance styling - dark theme compatible
const PROVENANCE_STYLES: Record<string, { border: string; bg: string; label: string }> = {
    'SyMRI':              { border: 'var(--mantine-color-violet-7)', bg: 'rgba(139, 92, 246, 0.1)', label: 'SyMRI' },
    'SWIRecon':           { border: 'var(--mantine-color-cyan-7)',   bg: 'rgba(34, 211, 238, 0.1)', label: 'SWI' },
    'EPIMix':             { border: 'var(--mantine-color-pink-6)',   bg: 'rgba(236, 72, 153, 0.1)', label: 'EPIMix' },
    'ProjectionDerived':  { border: 'var(--mantine-color-orange-7)', bg: 'rgba(251, 146, 60, 0.1)', label: 'Projections & MPRs' },
};

// Spinal cord styling
const SPINAL_CORD_STYLE = { border: 'var(--mantine-color-teal-6)', bg: 'rgba(20, 184, 166, 0.1)', label: 'Spinal Cord' };

// --- Stack Card Component ---
// Map flag types to badge colors (severity order: missing > conflict > low_confidence > ambiguous > review)
// Using vivid, distinct colors for easy differentiation
const FLAG_BADGE_COLORS: Record<AxisFlagType, string> = {
    missing: 'red.7',        // Bright red - most urgent
    conflict: 'orange.6',    // Vivid orange
    low_confidence: 'yellow.5', // Bright yellow
    ambiguous: 'violet.6',   // Purple/violet
    review: 'gray.6',        // Medium gray
};

// Parse manual_review_reasons_csv to determine most severe flag for CLASSIFICATION AXES ONLY
// Only consider flags for: base, technique, modifier, provenance, construct
const FLAG_SEVERITY: AxisFlagType[] = ['missing', 'conflict', 'low_confidence', 'ambiguous', 'review'];
const CLASSIFICATION_AXES = ['base', 'technique', 'modifier', 'provenance', 'construct'];

function getStackFlagType(reviewReasons: string | null): AxisFlagType | null {
    if (!reviewReasons) return null;
    
    // Parse CSV: "base:low_confidence,technique:conflict" -> check each reason
    const reasons = reviewReasons.split(',').map(r => r.trim().toLowerCase());
    
    // Only consider reasons that start with a classification axis
    const relevantReasons = reasons.filter(reason => 
        CLASSIFICATION_AXES.some(axis => reason.startsWith(axis + ':'))
    );
    
    if (relevantReasons.length === 0) return null;
    
    // Find the most severe flag among relevant reasons
    for (const flag of FLAG_SEVERITY) {
        if (relevantReasons.some(reason => reason.includes(flag))) {
            return flag;
        }
    }
    
    // If there are relevant reasons but no recognized flag type, default to 'review'
    return 'review';
}

interface StackCardProps {
    stack: StackItem;
    onClick: () => void;
    onQCClick: () => void;
}

const StackCard = ({ stack, onClick, onQCClick }: StackCardProps) => {
    const thumbnailUrl = `/api/qc/dicom/${stack.series_instance_uid}/thumbnail?stack_index=${stack.stack_index}&size=128`;
    // Get flag type directly from stack's review reasons (only classification axes)
    const qcFlagType = getStackFlagType(stack.manual_review_reasons_csv);
    const badgeColor = qcFlagType ? FLAG_BADGE_COLORS[qcFlagType] : 'cyan.6';
    
    return (
        <Card 
            padding="xs" 
            radius="sm" 
            withBorder
            onClick={onClick}
            style={{ cursor: 'pointer' }}
            styles={{ root: { '&:hover': { borderColor: 'var(--nils-accent-primary)' } } }}
        >
            {/* Thumbnail */}
            <Card.Section>
                <Box pos="relative">
                    <Image 
                        src={thumbnailUrl} 
                        height={100} 
                        fit="contain"
                        bg="dark.9"
                        fallbackSrc="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='100' height='100' viewBox='0 0 100 100'%3E%3Crect fill='%23333' width='100' height='100'/%3E%3Ctext fill='%23666' font-size='12' x='50%25' y='50%25' text-anchor='middle' dy='.3em'%3ENo Preview%3C/text%3E%3C/svg%3E"
                    />
                    {/* QC Badge - color indicates flag severity */}
                    <Tooltip label={qcFlagType ? `QC: ${qcFlagType.replace('_', ' ')}` : 'Open QC Editor'} withArrow position="right">
                        <Badge 
                            size="sm" 
                            pos="absolute" 
                            top={4} 
                            left={4}
                            variant="light"
                            color={badgeColor}
                            style={{ 
                                cursor: 'pointer', 
                                zIndex: 2,
                                padding: '3px 6px',
                                opacity: 0.85,
                            }}
                            onClick={(e) => {
                                e.stopPropagation();
                                onQCClick();
                            }}
                        >
                            <IconTarget size={12} />
                        </Badge>
                    </Tooltip>
                    {/* Slice count badge */}
                    <Badge 
                        size="xs" 
                        pos="absolute" 
                        bottom={4} 
                        right={4}
                        variant="filled"
                        color="dark"
                    >
                        {stack.slices_count}
                    </Badge>
                </Box>
            </Card.Section>
            
            {/* Stack Info */}
            <Group justify="space-between" mt="xs" gap={4}>
                <Text size="xs" fw={500} truncate style={{ flex: 1 }}>
                    {buildStackName(stack)}
                </Text>
                <StackBadges postContrast={stack.post_contrast} spinalCord={stack.spinal_cord} />
            </Group>
        </Card>
    );
};

// --- Intent Folder Component ---
interface IntentFolderProps {
    intent: string;
    stacks: StackItem[];
    defaultOpen?: boolean;
    onStackClick: (stack: StackItem) => void;
    onQCClick: (stack: StackItem) => void;
}

const IntentFolder = ({ 
    intent, 
    stacks, 
    defaultOpen,
    onStackClick,
    onQCClick,
}: IntentFolderProps) => {
    const [isOpen, setIsOpen] = useState(defaultOpen ?? false);
    
    // Sort by series_time (earliest first), then by stack_index
    const sortByTimeAndStack = (a: StackItem, b: StackItem) => {
        // First: sort by series_time (nulls go last)
        const timeA = a.series_time ?? 'zzz';  // null times sort last
        const timeB = b.series_time ?? 'zzz';
        if (timeA !== timeB) return timeA.localeCompare(timeB);
        
        // Second: sort by stack_index
        return a.stack_index - b.stack_index;
    };
    
    // Spinal cord stacks (not in provenance groups)
    const spinalCordStacks = stacks.filter(s => 
        s.spinal_cord === 1 && !PROVENANCE_STYLES[s.provenance || '']
    ).sort(sortByTimeAndStack);
    
    // Regular stacks (not spinal cord, not in provenance groups)
    const regularStacks = stacks.filter(s => 
        s.spinal_cord !== 1 && !PROVENANCE_STYLES[s.provenance || '']
    ).sort(sortByTimeAndStack);
    
    // Provenance groups (SyMRI, SWI, EPIMix)
    const provenanceGroups = Object.keys(PROVENANCE_STYLES)
        .map(p => ({ provenance: p, stacks: stacks.filter(s => s.provenance === p).sort(sortByTimeAndStack) }))
        .filter(g => g.stacks.length > 0);
    
    return (
        <Paper withBorder radius="md" shadow="xs">
            {/* Folder Header */}
            <UnstyledButton 
                w="100%" 
                p="sm" 
                bg="dark.6"
                onClick={() => setIsOpen(!isOpen)}
                style={{ 
                    borderBottom: isOpen ? '1px solid var(--mantine-color-dark-4)' : 'none',
                    borderRadius: isOpen ? '8px 8px 0 0' : '8px'
                }}
            >
                <Group justify="space-between">
                    <Group gap="sm">
                        <ThemeIcon variant="light" size="md">
                            <IconFolder size={16} />
                        </ThemeIcon>
                        <Text fw={600} tt="uppercase" size="sm">{intent}</Text>
                        <Badge size="xs" variant="light" color="gray">{stacks.length}</Badge>
                    </Group>
                    <IconChevronDown 
                        size={16} 
                        style={{ 
                            transform: isOpen ? 'rotate(180deg)' : 'rotate(0)', 
                            transition: 'transform 200ms ease' 
                        }} 
                    />
                </Group>
            </UnstyledButton>
            
            {/* Folder Content */}
            <Collapse in={isOpen}>
                <Box p="md">
                    {/* Regular stacks */}
                    {regularStacks.length > 0 && (
                        <SimpleGrid cols={{ base: 2, sm: 3, md: 4 }} mb={(provenanceGroups.length || spinalCordStacks.length) ? 'md' : 0}>
                            {regularStacks.map(stack => (
                                <StackCard 
                                    key={stack.series_stack_id} 
                                    stack={stack}
                                    onClick={() => onStackClick(stack)}
                                    onQCClick={() => onQCClick(stack)}
                                />
                            ))}
                        </SimpleGrid>
                    )}
                    
                    {/* Provenance-grouped stacks (SyMRI, SWI, EPIMix) */}
                    {provenanceGroups.map(({ provenance, stacks: pStacks }, idx) => (
                        <Box 
                            key={provenance}
                            p="sm" 
                            mt={(regularStacks.length > 0 || idx > 0) ? 'md' : 0}
                            mb="sm"
                            style={{ 
                                border: `2px solid ${PROVENANCE_STYLES[provenance].border}`,
                                borderRadius: 8,
                                background: PROVENANCE_STYLES[provenance].bg
                            }}
                        >
                            <Text size="xs" fw={600} mb="xs" c={PROVENANCE_STYLES[provenance].border}>
                                {PROVENANCE_STYLES[provenance].label}
                            </Text>
                            <SimpleGrid cols={{ base: 2, sm: 3, md: 4 }}>
                                {pStacks.map(stack => (
                                    <StackCard 
                                        key={stack.series_stack_id} 
                                        stack={stack}
                                        onClick={() => onStackClick(stack)}
                                        onQCClick={() => onQCClick(stack)}
                                    />
                                ))}
                            </SimpleGrid>
                        </Box>
                    ))}
                    
                    {/* Spinal Cord stacks (at the end) */}
                    {spinalCordStacks.length > 0 && (
                        <Box 
                            p="sm" 
                            mt={(regularStacks.length > 0 || provenanceGroups.length > 0) ? 'md' : 0}
                            style={{ 
                                border: `2px solid ${SPINAL_CORD_STYLE.border}`,
                                borderRadius: 8,
                                background: SPINAL_CORD_STYLE.bg
                            }}
                        >
                            <Text size="xs" fw={600} mb="xs" c={SPINAL_CORD_STYLE.border}>
                                {SPINAL_CORD_STYLE.label}
                            </Text>
                            <SimpleGrid cols={{ base: 2, sm: 3, md: 4 }}>
                                {spinalCordStacks.map(stack => (
                                    <StackCard 
                                        key={stack.series_stack_id} 
                                        stack={stack}
                                        onClick={() => onStackClick(stack)}
                                        onQCClick={() => onQCClick(stack)}
                                    />
                                ))}
                            </SimpleGrid>
                        </Box>
                    )}
                </Box>
            </Collapse>
        </Paper>
    );
};

// --- Main Component ---
export const QCViewerPage = ({ cohort, onBack }: QCViewerPageProps) => {
    // View State
    const [viewMode, setViewMode] = useState<'subjects' | 'sessions' | 'stacks'>('subjects');
    const [selectedSubject, setSelectedSubject] = useState<Subject | null>(null);
    const [selectedDate, setSelectedDate] = useState<string | null>(null);
    const [viewerStack, setViewerStack] = useState<StackItem | null>(null);
    const [qcStack, setQcStack] = useState<StackItem | null>(null);  // For QC modal

    // Search & Pagination State
    const [search, setSearch] = useState('');
    const [page, setPage] = useState(1);
    const [displayIdType, setDisplayIdType] = useLocalStorage<string>({
        key: 'qc-viewer-display-id-type',
        defaultValue: 'code',
    });
    const PAGE_SIZE = 50;


    // Queries
    const { data: subjectsData, isLoading: loadingSubjects } = useQuery({
        queryKey: ['qc', 'subjects', cohort.id, page, search, displayIdType],
        queryFn: () => apiClient.get<{ subjects: Subject[], total: number }>(`/qc/cohorts/${cohort.id}/subjects?limit=${PAGE_SIZE}&offset=${(page - 1) * PAGE_SIZE}&search=${search}&sort_by=${displayIdType}`),
        enabled: viewMode === 'subjects',
        placeholderData: keepPreviousData,
    });

    // Compute available ID types from the current page of subjects
    const availableIdTypes = ['code'];
    if (subjectsData?.subjects) {
        const types = new Set<string>();
        subjectsData.subjects.forEach(s => {
            if (s.other_ids) {
                s.other_ids.forEach(id => types.add(id.type));
            }
        });
        availableIdTypes.push(...Array.from(types).sort());
    }

    // Helper to get display label
    const getSubjectLabel = (subject: Subject) => {
        if (displayIdType === 'code') return subject.code;
        if (!subject.other_ids) return subject.code;
        const found = subject.other_ids.find(id => id.type === displayIdType);
        return found ? found.value : subject.code; // Fallback to code
    };

    const { data: sessionsData, isLoading: loadingSessions } = useQuery({
        queryKey: ['qc', 'sessions', selectedSubject?.id],
        queryFn: () => apiClient.get<{ sessions: Session[] }>(`/qc/subjects/${selectedSubject?.id}/sessions`),
        enabled: !!selectedSubject && viewMode === 'sessions',
    });

    const { data: stacksData, isLoading: loadingStacks } = useQuery({
        queryKey: ['qc', 'stacks', selectedSubject?.id, selectedDate],
        queryFn: () => apiClient.get<{ stacks: StackItem[] }>(`/qc/subjects/${selectedSubject?.id}/sessions/${encodeURIComponent(selectedDate || 'Unknown Date')}/stacks`),
        enabled: !!selectedSubject && !!selectedDate && viewMode === 'stacks',
    });

    // Handlers
    const handleSubjectClick = (subject: Subject) => {
        setSelectedSubject(subject);
        setViewMode('sessions');
    };

    const handleSessionClick = (date: string) => {
        setSelectedDate(date);
        setViewMode('stacks');
    };

    // --- Renderers ---

    const renderSubjects = () => (
        <Stack gap="md">
            <Group justify="space-between">
                <TextInput
                    placeholder="Search by code, ID, or alias..."
                    leftSection={<IconSearch size={16} />}
                    value={search}
                    onChange={(e) => { setSearch(e.currentTarget.value); setPage(1); }}
                    w={350}
                />

                <Group gap="xs">
                    <Text size="sm" c="dimmed">Display Identifier:</Text>
                    <Select
                        data={availableIdTypes.map(t => ({
                            value: t,
                            label: t === 'code' ? 'Subject Code' : t
                        }))}
                        value={displayIdType}
                        onChange={(val) => setDisplayIdType(val || 'code')}
                        w={180}
                        size="sm"
                        allowDeselect={false}
                    />
                </Group>
            </Group>

            <SimpleGrid cols={{ base: 1, sm: 2, md: 3, lg: 4 }}>
                {subjectsData?.subjects.map((subject: Subject) => (
                    <Card
                        key={subject.id}
                        padding="md"
                        radius="md"
                        withBorder
                        onClick={() => handleSubjectClick(subject)}
                        style={{ cursor: 'pointer' }}
                        styles={{ root: { '&:hover': { borderColor: 'var(--nils-accent-primary)' } } }}
                    >
                        <Group justify="space-between" align="flex-start" wrap="nowrap">
                            <Stack gap={0} style={{ minWidth: 0, flex: 1 }}>
                                <Tooltip label={displayIdType !== 'code' ? `Code: ${subject.code}` : 'Subject Code'} openDelay={500}>
                                    <Text fw={600} size="lg" truncate>
                                        {getSubjectLabel(subject)}
                                    </Text>
                                </Tooltip>
                                <Text size="xs" c="dimmed">ID: {subject.id}</Text>
                            </Stack>
                            <Badge variant="light">{subject.sex || '?'}</Badge>
                        </Group>
                    </Card>
                ))}
            </SimpleGrid>

            {subjectsData && subjectsData.total > PAGE_SIZE && (
                <Center mt="md">
                    <Pagination
                        total={Math.ceil(subjectsData.total / PAGE_SIZE)}
                        value={page}
                        onChange={setPage}
                    />
                </Center>
            )}
        </Stack>
    );

    const renderSessions = () => (
        <SimpleGrid cols={{ base: 1, sm: 2, md: 3 }}>
            {sessionsData?.sessions.map((session: Session) => (
                <Card
                    key={session.date}
                    padding="lg"
                    radius="md"
                    withBorder
                    onClick={() => handleSessionClick(session.date)}
                    style={{ cursor: 'pointer' }}
                    styles={{ root: { '&:hover': { borderColor: 'var(--nils-accent-primary)' } } }}
                >
                    <Stack gap="sm">
                        <Group gap="xs">
                            <IconCalendar size={20} color="var(--nils-accent-primary)" />
                            <Text fw={600} size="lg">{session.date}</Text>
                        </Group>
                        <Group gap="xs">
                            <IconFolder size={16} />
                            <Text size="sm">{session.study_count} Studies</Text>
                        </Group>
                        <Group gap={4}>
                            {session.modalities.slice(0, 3).map((mod: string) => (
                                <Badge key={mod} size="sm" variant="outline">{mod}</Badge>
                            ))}
                            {session.modalities.length > 3 && <Text size="xs">+{session.modalities.length - 3}</Text>}
                        </Group>
                    </Stack>
                </Card>
            ))}
        </SimpleGrid>
    );

    const renderStacks = () => {
        if (!stacksData?.stacks?.length) {
            return <Text c="dimmed">No stacks found for this session</Text>;
        }
        
        const groups = groupStacksByIntent(stacksData.stacks);
        const intents = Object.keys(groups);
        
        return (
            <Stack gap="md">
                {intents.map((intent, idx) => (
                    <IntentFolder
                        key={intent}
                        intent={intent}
                        stacks={groups[intent]}
                        defaultOpen={idx === 0}
                        onStackClick={(stack) => setViewerStack(stack)}
                        onQCClick={(stack) => setQcStack(stack)}
                    />
                ))}
            </Stack>
        );
    };

    // Breadcrumbs Logic
    const items = [
        { title: 'Cohorts', icon: IconFolders, onClick: onBack },
        { title: cohort.name, icon: IconFolder, onClick: () => { setViewMode('subjects'); setSelectedSubject(null); } },
    ];

    if (viewMode === 'sessions' || viewMode === 'stacks') {
        const subjectLabel = selectedSubject ? getSubjectLabel(selectedSubject) : 'Unknown Subject';
        items.push({
            title: subjectLabel,
            icon: IconUsers,
            onClick: () => { setViewMode('sessions'); setSelectedDate(null); }
        });
    }

    if (viewMode === 'stacks') {
        items.push({
            title: selectedDate || 'Unknown Date',
            icon: IconCalendarStats, // Matches card session icon
            onClick: () => { } // Current page
        });
    }

    return (
        <Stack gap="lg" h="100%">
            <Box>
                <Breadcrumbs separator="→" mt="xs">
                    {items.map((item, index) => {
                        const Icon = item.icon;
                        const isLast = index === items.length - 1;

                        return (
                            <Group key={index} gap={6} align="center">
                                <ThemeIcon size="sm" variant="light" color={isLast ? "blue" : "gray"}>
                                    <Icon size={14} />
                                </ThemeIcon>
                                {isLast ? (
                                    <Text size="sm" fw={600} c="var(--nils-text-primary)">
                                        {item.title}
                                    </Text>
                                ) : (
                                    <Anchor onClick={item.onClick} size="sm" fw={500} c="dimmed" style={{ display: 'flex', alignItems: 'center' }}>
                                        {item.title}
                                    </Anchor>
                                )}
                            </Group>
                        );
                    })}
                </Breadcrumbs>
            </Box>


            <Box style={{ flex: 1 }}>
                {viewMode === 'subjects' && (loadingSubjects ? <Loader /> : renderSubjects())}
                {viewMode === 'sessions' && (loadingSessions ? <Loader /> : renderSessions())}
                {viewMode === 'stacks' && (loadingStacks ? <Loader /> : renderStacks())}
            </Box>

            {/* Dicom Viewer Modal */}
            <Modal
                opened={!!viewerStack}
                onClose={() => setViewerStack(null)}
                fullScreen
                title={viewerStack ? buildStackName(viewerStack) : ''}
                styles={{ body: { height: 'calc(100vh - 60px)' } }}
            >
                {viewerStack && (
                    <Box h="100%">
                        <DicomViewer
                            seriesUid={viewerStack.series_instance_uid}
                            stackIndex={viewerStack.stack_index}
                            maxHeight={window.innerHeight - 100}
                        />
                    </Box>
                )}
            </Modal>

            {/* Single Stack QC Modal */}
            {qcStack && (
                <SingleStackQCModal
                    cohortId={cohort.id}
                    stackId={qcStack.series_stack_id}
                    seriesUid={qcStack.series_instance_uid}
                    stackIndex={qcStack.stack_index}
                    stackName={buildStackName(qcStack)}
                    opened={!!qcStack}
                    onClose={() => setQcStack(null)}
                />
            )}
        </Stack>
    );
};
