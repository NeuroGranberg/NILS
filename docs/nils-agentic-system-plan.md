# NILS Agentic Research Assistant - Implementation Plan

> **Audience**: This is the **developer implementation spec** — technology choices,
> LangGraph internals, query library design, Docker setup, and the phase roadmap.
>
> For the **agent operational guide** (system prompt content, tool usage, query patterns,
> behavioral rules), see [`nils-agent-guide.md`](./nils-agent-guide.md).

## Executive Summary

This document outlines the architecture for building an intelligent research assistant for NILS (Neuroimaging Intelligent Linked System). The system will enable researchers to interact with neuroimaging data through natural language, run complex queries, start processing jobs, visualize results, and iteratively refine research questions across multiple chat sessions.

---

## 1. NILS Data Model Overview

### 1.1 Database Architecture

NILS uses **two PostgreSQL databases**:

| Database | Purpose | Key Tables |
|----------|---------|------------|
| **metadata-db** (port 5532) | DICOM metadata, clinical data, classifications | 26 tables, 32M+ instances |
| **db** (port 5432) | Application state: jobs, cohorts, QC, pipeline | 9 tables |

### 1.2 Metadata Database - Core Entities

```
Subject (6,962 subjects)
├── Study (32,993 sessions spanning 2001-2025)
│   ├── Series (355,596 acquisitions)
│   │   ├── SeriesStack (468,010 homogeneous groups)
│   │   │   ├── StackFingerprint (physics parameters)
│   │   │   └── SeriesClassificationCache (6-axis classification)
│   │   └── Instance (32M+ DICOM files)
│   └── event_id → Event (MRI Scan event)
├── SubjectDisease → Disease (MS, ALS, Parkinson, etc.)
│   ├── onset_event_id → Event (Disease Onset)
│   ├── diagnosis_event_id → Event (Diagnosis date)
│   └── SubjectDiseaseType → DiseaseType (RRMS, SPMS, PPMS, etc.)
├── SubjectCohort → Cohort (iaid, als, kipro)
├── NumericMeasure → ObservationType (EDSS, SDMT, Weight, Height)
└── Event (147,867 clinical events with dates)
```

### 1.3 Real Data Statistics

| Entity | Count | Notes |
|--------|-------|-------|
| Subjects | 6,962 | Anonymized subject codes |
| Studies (Sessions) | 32,993 | 2001-01-08 to 2025-06-05 |
| Series | 355,596 | MRI acquisitions |
| Instances | 32,093,922 | Individual DICOM files |
| Events | 147,867 | MRI Scan, EDSS, Diagnosis, Onset, etc. |
| EDSS/SDMT measures | 100,181 | Clinical scores linked to events |
| Diseases | 8 | MS, ALS, Parkinson, NMOSD, MOGAD, Alzheimer, Dementia, Control |
| Disease Types | 17 | CIS, RRMS, SPMS, PPMS, Limb-onset ALS, etc. |
| Cohorts | 3 | iaid, als, kipro |

### 1.4 Field Strengths & Manufacturers

| Field Strength | Series Count |
|----------------|--------------|
| 1.5T | 199,667 |
| 3T | 138,056 |
| 7T | 12,689 |

| Manufacturer | Dominant |
|--------------|----------|
| SIEMENS | ~70% |
| GE | ~25% |
| PHILIPS | ~5% |

### 1.5 Six-Axis Classification System

Each SeriesStack is classified on 6 orthogonal axes:

| Axis | Examples | Top Values in Database |
|------|----------|----------------------|
| **Base** | T1w, T2w, DWI, SWI, PDw | T1w (56K), T2w (45K), DWI (20K) |
| **Technique** | MPRAGE, TSE, FLASH, SPACE | FLASH, TSE, MPRAGE, SPACE |
| **Modifier** | FLAIR, FatSat, 3D, STIR | FLAIR+FatSat, FLAIR, STIR |
| **Construct** | ADC, FA, T1Map, SyntheticT1w | ADC, FA, T1Map |
| **Provenance** | SyMRI, SWIRecon, RawRecon | RawRecon (default) |
| **Acceleration** | GRAPPA, SMS, CS | GRAPPA |

---

## 2. Query Complexity Analysis

### 2.1 Example Complex Queries

The agent must handle queries like:

**Query 1**: "Subjects with at least 2 sessions between 2003-2015"
```sql
-- Requires: subject, study, date filtering, HAVING COUNT
```

**Query 2**: "...with at least 3 sessions, each 2+ years apart"
```sql
-- Requires: window functions (LAG), interval calculations
```

**Query 3**: "...with EDSS within 3 months of every session"
```sql
-- Requires: temporal join between study and numeric_measures via events
-- Complex: "every session" needs ALL sessions to have nearby EDSS
```

**Query 4**: "...with late onset MS (diagnosed after age 50)"
```sql
-- Requires: subject_diseases → onset_event_id → event → date
-- Plus: subject.patient_birth_date for age calculation
```

**Query 5**: "...with BMI > 30"
```sql
-- Requires: Weight AND Height measures, compute BMI = weight/(height^2)
-- Note: Only 2,573 subjects have both measurements
```

**Query 6**: "...only 3T images with T1-3D AND T2-FLAIR"
```sql
-- Requires: mri_series_details.magnetic_field_strength = 3
-- Plus: series_classification_cache.base/technique/modifier
-- Join through series → study for session-level filtering
```

**Query 7**: "...sessions with identical T1-3D acquisitions"
```sql
-- Requires: stack_fingerprint comparison
-- Match: mr_tr, mr_te, mr_ti, mr_flip_angle, fov_x, fov_y, orientation
```

### 2.2 Why Direct LLM-to-SQL Fails

1. **7+ table joins** for a single query
2. **Indirect relationships** (onset_event_id → event → date)
3. **Domain terminology** ("late onset" = age > 45-50 at onset_event)
4. **Temporal reasoning** ("within 3 months" = ABS(date1 - date2) <= 90)
5. **All/Every semantics** ("EDSS for EVERY session" ≠ "EDSS for ANY session")
6. **Classification vocabulary** (T1-3D = base='T1w' + technique='MPRAGE' OR modifier LIKE '%3D%')

---

## 3. Recommended Architecture

### 3.1 Technology Stack

| Component | Technology | Rationale |
|-----------|------------|-----------|
| **Agent Framework** | **Deep Agents** (LangChain) | Built on LangGraph, has planning, subagents, long-term memory, file system tools. Provides the Tier 2 execution model (write, run, error-recover, iterate). |
| **LLM Gateway** | **CLIProxyAPI** | Free access to Claude/GPT via OAuth subscriptions. Multi-account load balancing. No API costs. 10.3K stars. |
| **Query Library** | **Python function library** (`backend/src/agent/query_library/`) | Pre-built, tested, parameterized functions for known query patterns (Tier 1). Encodes domain knowledge (joins, normalization, physics filters, stack priority) so the agent doesn't need to re-derive them. |
| **Semantic Layer** | **None (runtime)** | Cube was evaluated and removed from the agent runtime. Scoping counts → `run_query("scoping_count")`. Schema discovery → `run_query("schema_discovery")`. Both call metadata-db directly. Cube may be deployed separately as a developer UI with no agent dependency. |
| **Visualization** | **Plotly.js** (`react-plotly.js`) | Rich chart types including Sankey (confirmed requirement). Built-in interactivity (zoom, pan, lasso). Familiar to scientific Python users. See Section 3.3 for tradeoff mitigations. |
| **Memory Backend** | **PostgreSQL** | Reuse existing `app-db` (DATABASE_URL). PostgresStore for long-term memory. PostgresSaver for thread checkpoints. |

### 3.2 Why These Choices

#### Deep Agents vs LangGraph vs AutoGen

| Aspect | Deep Agents | Raw LangGraph | AutoGen |
|--------|-------------|---------------|---------|
| Abstraction | High-level harness | Low-level runtime | Multi-agent conversation |
| Built-in planning | ✓ (write_todos tool) | Manual | ✓ |
| Long-term memory | ✓ (/memories/ paths) | Manual Store setup | Different architecture |
| Subagent spawning | ✓ (task tool) | Manual | ✓ (agent handoffs) |
| Best for | Multi-tool orchestration | Custom workflows | Agent-to-agent chat |
| Your use case | **Best fit** | More code needed | Overkill for 1 agent |

#### CLIProxyAPI vs LiteLLM

| Aspect | CLIProxyAPI | LiteLLM |
|--------|-------------|---------|
| Model access | Free via Claude/Codex/Gemini subscriptions | Paid API keys |
| Multi-account | Built-in round-robin load balancing | Failover only |
| Academic use | **Ideal** - no per-query cost | Expensive at scale |
| Auth model | OAuth to CLI tools | API key management |

#### Two-Tier Execution: Query Library + Deep Agents

Analysis of all query sessions revealed that virtually every real research query
requires at least one of: voxel physics calculations, same-scanner joins, stateful
temporal logic, sequential event comparisons, or multi-tier evidence hierarchies —
none of which a semantic layer can express.

The solution is a two-tier model:

**Tier 1 — Query Library** (covers ~70-80% of requests):
- Pre-built Python functions, each encapsulating one research query pattern
- Parameterized: `run_query("modality_sessions", {"cohort": "stopms", "field_strength": [3.0], ...})`
- Handles all internal complexity (joins, normalization, psycopg escaping, stack priority)
- Returns structured `QueryResult` with stepwise funnel built in
- Populated from temp/ query sessions — we already have the hard SQL; it just needs wrapping

**Tier 2 — Deep Agents execution** (covers the rest):
- Agent writes SQL and/or Python iteratively, executes it, reads errors, recovers
- Full Deep Agents model: planning, subagents, file tools, error recovery loop
- Correct for novel queries, complex temporal anchors, composite logic
- Behavioral rules from nils-agent-guide.md (parallel discovery, "Surprised? Investigate") apply here

**Cube — removed from agent runtime:**
After analyzing all query sessions, Cube was removed from the agent's execution path.
Every real research query required physics calculations, temporal anchors, or stateful
logic that no semantic layer can express. Scoping counts and schema discovery are
handled by two library functions (`scoping_count`, `schema_discovery`) that call
metadata-db directly. Cube may optionally be deployed as a standalone developer
exploration UI with no agent code dependency.

| Without library (LLM writes all SQL) | With library (Tier 1 + Tier 2) |
|--------------------------------------|--------------------------------|
| LLM must derive 26-table join paths from scratch | 70-80% of queries: select + parameterize a function |
| psycopg escaping errors, NULL propagation bugs | Errors handled inside tested functions |
| Every complex query requires iterative debugging | Debugging effort concentrated in Tier 2 only |
| No separation between "known pattern" and "novel" | Clear escalation path; novel queries still fully supported |

#### Why Plotly.js over Vega-Lite

| Criterion | Plotly.js | Vega-Lite | Winner for NILS |
|-----------|-----------|-----------|-----------------|
| **Sankey diagrams** | First-class `sankey` trace type (confirmed requirement from query sessions) | No native support; requires dropping to raw Vega | **Plotly.js** |
| **Chart type breadth** | 50+ trace types: Sankey, violin, 3D surface, contour, SPLOM | Covers common statistical charts but lacks Sankey, 3D, niche types | **Plotly.js** |
| **Interactivity** | Zoom, pan, hover, click, lasso select, export -- all built-in | Basic zoom/pan/hover; advanced interactions need raw Vega | **Plotly.js** |
| **Scientific familiarity** | Python Plotly Express is standard in neuroimaging/scientific computing; JSON carries over | More academic/grammar-of-graphics oriented | **Plotly.js** |
| **React integration** | `react-plotly.js` -- mature, well-maintained | `react-vega` -- works but less actively maintained | **Plotly.js** |
| **LLM spec generation** | LLMs slightly less accurate (imperative API, multiple ways to do same thing) | Declarative JSON grammar, more consistent LLM output | Vega-Lite |
| **Spec validation** | No formal JSON Schema; must render to verify | Formal JSON Schema for pre-render validation | Vega-Lite |
| **Incremental patching** | Requires knowing trace index and layout path | JSON merge-patch works naturally on declarative spec | Vega-Lite |
| **Bundle size** | ~1MB minified (mitigated with partial bundles, see below) | ~350KB minified (vega + vega-lite + vega-embed) | Vega-Lite |
| **Security (LLM output)** | Specs can include JS callbacks; must sanitize | Pure declarative JSON, no code execution | Vega-Lite |

**Decision**: Plotly.js -- the Sankey requirement is a hard blocker for Vega-Lite, and Plotly's chart breadth and interactivity are better suited for exploratory neuroimaging research. The tradeoffs are all mitigable (see Section 3.3).

### 3.3 Plotly.js Tradeoff Mitigations

Plotly.js has known tradeoffs compared to Vega-Lite. Here is how we address each one:

#### 3.3.1 LLM Spec Generation Accuracy

**Problem**: LLMs produce less consistent Plotly specs than Vega-Lite because Plotly's API is imperative with multiple ways to achieve the same result.

**Mitigation: Chart Template Library**

The agent system prompt includes a library of validated Plotly figure templates for every chart type used in NILS. The LLM fills in data and parameters rather than generating specs from scratch.

```python
# backend/src/agent/tools/chart_templates.py
CHART_TEMPLATES = {
    "line_progression": {
        "description": "EDSS/SDMT progression over time per subject",
        "template": {
            "data": [{
                "type": "scatter",
                "mode": "lines+markers",
                "x": "__DATES__",
                "y": "__VALUES__",
                "name": "__SUBJECT__",
                "line": {"width": 2}
            }],
            "layout": {
                "xaxis": {"title": "__X_LABEL__"},
                "yaxis": {"title": "__Y_LABEL__", "range": "__Y_RANGE__"},
                "template": "plotly_white",
                "hovermode": "x unified"
            }
        }
    },
    "sankey_transitions": {
        "description": "Treatment or disease-type transitions between time points",
        "template": {
            "data": [{
                "type": "sankey",
                "orientation": "h",
                "node": {"label": "__LABELS__", "pad": 15, "thickness": 20},
                "link": {"source": "__SOURCES__", "target": "__TARGETS__", "value": "__VALUES__"}
            }],
            "layout": {"title": "__TITLE__", "template": "plotly_white"}
        }
    },
    "histogram_distribution": {
        "description": "Distribution of sessions, intervals, EDSS scores, etc.",
        "template": {
            "data": [{
                "type": "bar",
                "x": "__CATEGORIES__",
                "y": "__COUNTS__",
                "text": "__COUNTS__",
                "textposition": "outside",
                "texttemplate": "%{y}",
                "marker": {"color": "#1f77b4"}
            }],
            "layout": {
                "xaxis": {"title": "__X_LABEL__", "type": "category"},
                "yaxis": {"title": "Count"},
                "template": "plotly_white"
            }
        }
    },
    "scatter_comparison": {
        "description": "Scatter plot comparing two measures (e.g., EDSS vs SDMT)",
        "template": {
            "data": [{
                "type": "scatter",
                "mode": "markers",
                "x": "__X_VALUES__",
                "y": "__Y_VALUES__",
                "text": "__HOVER_TEXT__",
                "marker": {"size": 8, "opacity": 0.7}
            }],
            "layout": {
                "xaxis": {"title": "__X_LABEL__"},
                "yaxis": {"title": "__Y_LABEL__"},
                "template": "plotly_white"
            }
        }
    },
    "grouped_bar": {
        "description": "Grouped bar chart (e.g., sex distribution by disease type)",
        "template": {
            "data": [],
            "layout": {
                "barmode": "group",
                "xaxis": {"title": "__X_LABEL__"},
                "yaxis": {"title": "Count"},
                "template": "plotly_white"
            }
        }
    }
}
```

The agent selects a template, fills placeholders with query results, and can apply user-requested modifications on top. This constrains the LLM to known-good spec structures.

#### 3.3.2 Spec Validation (No Formal JSON Schema)

**Problem**: Unlike Vega-Lite, Plotly has no formal JSON Schema to validate specs before rendering.

**Mitigation: Server-Side Validation Layer**

```python
# backend/src/agent/tools/plotly_validator.py
VALID_TRACE_TYPES = {
    "scatter", "bar", "sankey", "histogram", "box", "violin",
    "heatmap", "contour", "pie", "sunburst", "treemap", "table"
}

REQUIRED_FIELDS = {
    "scatter": ["x", "y"],
    "bar": ["x", "y"],
    "sankey": ["node", "link"],
    "histogram": ["x"],
}

def validate_plotly_spec(spec: dict) -> tuple[bool, list[str]]:
    """Validate a Plotly figure spec before sending to frontend."""
    errors = []
    
    if "data" not in spec or not isinstance(spec["data"], list):
        errors.append("Missing or invalid 'data' array")
        return False, errors
    
    for i, trace in enumerate(spec["data"]):
        trace_type = trace.get("type", "scatter")
        if trace_type not in VALID_TRACE_TYPES:
            errors.append(f"Trace {i}: unknown type '{trace_type}'")
            continue
        
        for field in REQUIRED_FIELDS.get(trace_type, []):
            if field not in trace:
                errors.append(f"Trace {i} ({trace_type}): missing required field '{field}'")
    
    if "layout" in spec and not isinstance(spec["layout"], dict):
        errors.append("'layout' must be a dict")
    
    return len(errors) == 0, errors
```

Every LLM-generated spec passes through this validator before reaching the frontend. If validation fails, the agent retries with the error message as context.

#### 3.3.3 Incremental Patching

**Problem**: Modifying an existing Plotly chart requires knowing trace indices and nested layout paths, unlike Vega-Lite's natural JSON merge-patch.

**Mitigation: Structured Patch Operations**

The `modify_visualization` tool accepts high-level patch instructions that the agent translates into precise Plotly updates:

```python
# backend/src/agent/tools/plotly_patcher.py
import copy

def apply_plotly_patch(current_spec: dict, patch: dict) -> dict:
    """Apply a patch to an existing Plotly figure spec.
    
    Supports:
    - layout updates: {"layout": {"title": "New Title"}}
    - trace-level updates: {"data": [{"line": {"width": 3}}]}  (applies to all traces)
    - indexed trace updates: {"data": {0: {"marker": {"color": "red"}}}}
    - colorway: {"layout": {"colorway": [...]}}
    """
    result = copy.deepcopy(current_spec)
    
    if "layout" in patch:
        result["layout"] = deep_merge(result.get("layout", {}), patch["layout"])
    
    if "data" in patch:
        if isinstance(patch["data"], list):
            # Apply same patch to all traces
            for trace in result["data"]:
                for patch_trace in patch["data"]:
                    deep_merge(trace, patch_trace)
        elif isinstance(patch["data"], dict):
            # Apply to specific trace indices
            for idx_str, trace_patch in patch["data"].items():
                idx = int(idx_str)
                if idx < len(result["data"]):
                    deep_merge(result["data"][idx], trace_patch)
    
    return result

def deep_merge(base: dict, override: dict) -> dict:
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base
```

The agent stores the current figure spec in thread state, applies patches incrementally, and sends the full updated spec to the frontend. The user experiences seamless "make it blue" type refinements.

#### 3.3.4 Bundle Size

**Problem**: Full `plotly.js-dist` is ~1MB minified (~3.4MB unminified), significantly larger than Vega-Lite's ~350KB.

**Mitigation: Partial Bundle + Lazy Loading**

```javascript
// frontend/src/features/chat/PlotlyRenderer.tsx
// Use plotly.js-basic-dist-min (~300KB) for common charts,
// dynamically import full bundle only when Sankey/3D is needed.

import Plotly from 'plotly.js-basic-dist-min';  // bar, scatter, pie, histogram
import createPlotlyComponent from 'react-plotly.js/factory';

const Plot = createPlotlyComponent(Plotly);

// For Sankey/advanced charts, lazy-load the full bundle
const ADVANCED_TRACE_TYPES = new Set(['sankey', 'sunburst', 'treemap', 'contour', 'surface']);

function needsFullPlotly(spec: PlotlySpec): boolean {
  return spec.data.some(trace => ADVANCED_TRACE_TYPES.has(trace.type));
}

// Lazy-loaded full Plotly (only fetched when Sankey etc. is used)
const FullPlot = React.lazy(() =>
  import('plotly.js-dist-min').then(Plotly => ({
    default: createPlotlyComponent(Plotly.default)
  }))
);
```

This gives us ~300KB for the common case (bar, line, scatter, histogram) and only loads the full ~1MB bundle when Sankey or other advanced chart types are requested.

#### 3.3.5 Security (LLM-Generated Specs)

**Problem**: Plotly specs can include JavaScript callbacks (e.g., in `customdata`, event handlers) which could be a vector for code injection from LLM output.

**Mitigation: Server-Side Sanitization**

```python
# backend/src/agent/tools/plotly_validator.py (addition)
import re

DANGEROUS_KEYS = {"customdata", "transforms", "frames"}
JS_PATTERN = re.compile(r'function\s*\(|=>\s*{|eval\(|new Function', re.IGNORECASE)

def sanitize_plotly_spec(spec: dict) -> dict:
    """Remove any potentially executable content from LLM-generated specs."""
    return _sanitize_recursive(spec)

def _sanitize_recursive(obj):
    if isinstance(obj, dict):
        return {
            k: _sanitize_recursive(v)
            for k, v in obj.items()
            if k not in DANGEROUS_KEYS
        }
    elif isinstance(obj, list):
        return [_sanitize_recursive(item) for item in obj]
    elif isinstance(obj, str):
        if JS_PATTERN.search(obj):
            return ""  # Strip any string containing JS patterns
        return obj
    return obj
```

All LLM-generated specs are sanitized server-side before being sent to the frontend. The frontend `react-plotly.js` component renders with `staticPlot: false` for interactivity but never executes arbitrary code from the spec.

#### 3.3.6 Summary of Mitigations

| Tradeoff | Mitigation | Residual Risk |
|----------|------------|---------------|
| LLM accuracy | Chart template library constrains output to known-good structures | Minimal -- novel chart types may need a new template |
| No JSON Schema | Custom server-side validator checks structure before render | Low -- covers common errors; exotic issues caught by frontend |
| Incremental patching | Structured patcher with deep-merge and trace indexing | Low -- handles all common "make it X" requests |
| Bundle size (~1MB) | Partial bundle (300KB base) + lazy-load full bundle for Sankey/3D | None -- equivalent to Vega-Lite for common charts |
| Security | Server-side sanitizer strips JS callbacks and dangerous keys | Minimal -- defense-in-depth with frontend CSP |

---

## 4. Memory Architecture

### 4.1 Database Separation

**Critical**: Agent memory uses `app-db` (neurotoolkit), NOT `metadata-db`:

| Database | Port | Purpose | Agent Access |
|----------|------|---------|--------------|
| **app-db** (neurotoolkit) | 5432 | Jobs, cohorts, **agent memory** | Read/Write |
| **metadata-db** (neurotoolkit_metadata) | 5532 | DICOM metadata (32M+ instances) | **Read-only** via query library (Tier 1) or direct SQL (Tier 2) |

```
┌─────────────────────────────────────────────────────────────────┐
│                   app-db (neurotoolkit) - port 5432             │
├─────────────────────────────────────────────────────────────────┤
│  EXISTING TABLES:                                               │
│  - jobs, job_stages                                             │
│  - cohorts, cohort_subjects                                     │
│                                                                 │
│  NEW AGENT TABLES (via LangGraph migrations):                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ checkpoints          │ Thread state snapshots (short-term)  ││
│  │ checkpoint_blobs     │ Serialized graph state               ││
│  │ checkpoint_writes    │ Pending writes (fault tolerance)     ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ store                │ Long-term memory with namespaces     ││
│  │                      │ - ("user", user_id, "preferences")   ││
│  │                      │ - ("user", user_id, "saved_queries") ││
│  │                      │ - ("global", "patterns")             ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              metadata-db (neurotoolkit_metadata) - port 5532    │
│                      READ-ONLY FOR AGENT                        │
├─────────────────────────────────────────────────────────────────┤
│  Subject, Study, Series, SeriesStack, Instance (32M+)           │
│  Event, Disease, Observation, Measure, etc.                     │
│  (Agent queries via query library / direct SQL — never writes)  │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 LangGraph Memory Components

LangGraph provides two distinct memory systems:

| Component | Class | Purpose | Tables Created |
|-----------|-------|---------|----------------|
| **Checkpointer** | `PostgresSaver` | Short-term: conversation state per thread | `checkpoints`, `checkpoint_blobs`, `checkpoint_writes` |
| **Store** | `PostgresStore` | Long-term: cross-conversation memories | `store` |

**Key Distinction**:
- **PostgresSaver** = thread-scoped (like RAM per browser tab)
- **PostgresStore** = user/global-scoped (like disk storage)

### 4.3 Three Memory Layers

| Layer | Scope | Backend | Example |
|-------|-------|---------|---------|
| **Intra-turn** | Current graph execution | In-memory `State` object | "Change that chart" → knows which chart |
| **Thread (Chat)** | Per conversation | `PostgresSaver` (checkpoints) | Full message history for this chat session |
| **Long-term** | Cross-conversation | `PostgresStore` (store table) | User prefers blue charts, successful query patterns |

### 4.4 Implementation

```python
# backend/src/agent/memory.py
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.postgres.aio import AsyncPostgresStore
from langgraph.graph import StateGraph

async def create_agent(db_url: str):
    """
    Initialize agent with memory using existing DATABASE_URL.
    No new environment variable needed.
    """
    # Short-term: conversation state per thread
    async with AsyncPostgresSaver.from_conn_string(db_url) as checkpointer:
        await checkpointer.setup()  # Creates checkpoints tables (run once)
        
        # Long-term: cross-conversation memories
        async with AsyncPostgresStore.from_conn_string(db_url) as store:
            await store.setup()  # Creates store table (run once)
            
            builder = StateGraph(...)
            graph = builder.compile(
                checkpointer=checkpointer,
                store=store
            )
            return graph

# Usage in FastAPI
from src.db.engine import DATABASE_URL  # Existing app-db connection

graph = await create_agent(DATABASE_URL)
```

### 4.5 Long-term Memory Namespace Design

```python
# Namespace hierarchy for PostgresStore
NAMESPACES = {
    # User-specific memories (isolated per user)
    ("user", "{user_id}", "preferences"):     # Chart colors, default filters
    ("user", "{user_id}", "saved_queries"):   # Successful complex queries
    ("user", "{user_id}", "corrections"):     # "Late onset means >50 not >45"
    
    # Global patterns (shared across users)
    ("global", "query_patterns"):             # Common successful SQL patterns
    ("global", "domain_terminology"):         # Learned neuroimaging terms
    ("global", "cohort_definitions"):         # Standard cohort criteria
}
```

### 4.6 Accessing Memory in Agent Nodes

```python
from dataclasses import dataclass
from langgraph.runtime import Runtime
import uuid

@dataclass
class AgentContext:
    user_id: str

async def query_node(state: MessagesState, runtime: Runtime[AgentContext]):
    user_id = runtime.context.user_id
    
    # 1. Search long-term memory for similar successful queries
    similar = await runtime.store.asearch(
        ("user", user_id, "saved_queries"),
        query=state["messages"][-1].content,
        limit=3
    )
    
    # 2. Get user preferences
    prefs = await runtime.store.asearch(
        ("user", user_id, "preferences"),
        query="visualization",
        limit=5
    )
    
    # ... execute query ...
    
    # 3. Save successful query pattern
    if query_succeeded:
        await runtime.store.aput(
            ("user", user_id, "saved_queries"),
            str(uuid.uuid4()),
            {
                "natural_language": state["messages"][-1].content,
                "tier": tier,
                "sql_or_function": sql_or_function,
                "result_count": result_count,
                "timestamp": datetime.now().isoformat(),
            }
        )
    
    return {"messages": [response]}
```

### 4.7 Database Bloat Prevention

LangGraph checkpoints can cause significant database growth. Implement periodic cleanup:

```python
# backend/src/agent/maintenance.py
from datetime import datetime, timedelta

async def cleanup_old_checkpoints(checkpointer, days_old: int = 30):
    """Delete checkpoints older than N days. Run weekly via cron/scheduler."""
    cutoff = datetime.now() - timedelta(days=days_old)
    # Use checkpointer's delete methods
    # Note: Exact API depends on langgraph-checkpoint-postgres version
    
# Add to existing job scheduler or as a management command
```

### 4.8 Memory Update Triggers

| Trigger | Action | Namespace |
|---------|--------|-----------|
| User says "perfect", "exactly right" | Save query pattern | `(user, id, saved_queries)` |
| User corrects agent | Record correction | `(user, id, corrections)` |
| User states preference | Update preference | `(user, id, preferences)` |
| Complex query succeeds globally | Add to patterns | `(global, query_patterns)` |

---

## 5. Agent Tools

### 5.1 Data Query Tools

| Tool | Tier | Description | Calls |
|------|------|-------------|-------|
| `run_query(function, params)` | **1** | Execute a pre-built query library function (includes `scoping_count`, `schema_discovery`) | `backend/src/agent/query_library/` → metadata-db |
| `execute_sql(sql, params)` | **2** | Agent-written SQL, direct execution | Direct psycopg → metadata-db |
| `execute_python(code)` | **2** | Agent-written Python (can call execute_sql internally) | Sandboxed execution |

### 5.2 Job Management Tools

| Tool | Description | Calls |
|------|-------------|-------|
| `list_jobs` | List all jobs with status | GET /api/jobs |
| `get_job_status` | Get specific job details | GET /api/jobs/{id} |
| `start_extraction` | Start DICOM extraction job | POST /api/cohorts/{id}/extract |
| `start_sorting` | Start classification job | POST /api/cohorts/{id}/sort |
| `pause_job` | Pause running job | POST /api/jobs/{id}/pause |
| `resume_job` | Resume paused job | POST /api/jobs/{id}/resume |
| `cancel_job` | Cancel job | POST /api/jobs/{id}/cancel |

### 5.3 Cohort Tools

| Tool | Description | Calls |
|------|-------------|-------|
| `list_cohorts` | List available cohorts | Cohort table query |
| `get_cohort_metrics` | Subject/study/series counts | metadata_db.metrics |
| `create_cohort` | Create new cohort | POST /api/cohorts |

### 5.4 Visualization Tools

| Tool | Description | Output |
|------|-------------|--------|
| `create_visualization` | Generate Plotly.js figure spec from data | JSON figure `{data, layout}` |
| `modify_visualization` | Patch existing chart (colors, labels, etc.) | Updated JSON figure |
| `suggest_chart_type` | Recommend visualization for data type | Chart type + rationale |

### 5.5 Memory Tools

| Tool | Description |
|------|-------------|
| `read_memory` | Read from PostgresStore by namespace + key |
| `write_memory` | Write/update PostgresStore (namespace, key, content) |
| `search_memory` | Semantic search across PostgresStore |

---

## 6. Implementation Phases

### Phase 1: Foundation + Query Library (Week 1-2)

The query library is a **Phase 1 deliverable**, not an afterthought. The temp/ query
sessions have already produced all the hard SQL; this phase wraps it into
parameterized, tested functions.

1. **Add CLIProxyAPI to docker-compose.yml**
   ```yaml
   cliproxyapi:
     image: ghcr.io/router-for-me/cli-proxy-api:latest
     ports:
       - "127.0.0.1:8080:8080"
     volumes:
       - ./resource/cliproxy/config.yaml:/app/config.yaml
       - ./resource/cliproxy/auths:/app/auths
   ```

2. **Add Deep Agents dependencies to pyproject.toml**
   ```toml
   "deepagents>=0.4",
   "langgraph-checkpoint-postgres>=0.1",
   "langchain-openai>=0.3",
   ```

3. **Create agent module structure**
   ```
   backend/src/agent/
   ├── __init__.py
   ├── agent.py               # Deep Agents setup
   ├── prompts.py             # System prompt (loads nils-agent-guide.md)
   ├── query_library/         # TIER 1 — pre-built query functions
   │   ├── __init__.py        # run_query(function, params) dispatcher
   │   ├── models.py          # QueryResult, FunnelStep dataclasses
   │   ├── modality.py        # modality_sessions, multimodal_cooccurrence
   │   ├── longitudinal.py    # longitudinal_anchor (DMT, pregnancy, SPMS)
   │   ├── intervals.py       # interval_groups
   │   ├── clinical.py        # clinical_proximity, cohort_demographics
   │   ├── treatments.py      # treatment_sequences, treatment_transitions
   │   └── export.py          # stack_export, schema_discovery
   └── tools/
       ├── __init__.py
       ├── query.py           # run_query, execute_sql, execute_python
       ├── jobs.py            # job management tools
       ├── cohorts.py         # cohort tools
       └── visualize.py       # Plotly.js tools
   ```

4. **Build query library** — port from temp/ scripts:
   - `modality_sessions` ← stopms/iaid T1+FLAIR interval queries
   - `interval_groups` ← STOPMS 3D interval analysis
   - `longitudinal_anchor` ← DMT cohort, pregnancy cohort, SPMS converters
   - `treatment_sequences` / `treatment_transitions` ← treatment Sankey queries
   - `clinical_proximity` ← EDSS/SDMT within ±N days
   - `cohort_demographics` ← age, sex, disease type per cohort
   - `stack_export` ← final stack selection with priority + provenance filter

5. **Add SSE endpoint to FastAPI**
   ```python
   @router.get("/api/chat/stream")
   async def chat_stream(thread_id: str, message: str):
       # SSE streaming response
   ```

### Phase 2: Memory System (Week 3-4)

Cube is not deployed as part of the agent stack. If wanted for manual developer
exploration, it can be added to a separate `docker-compose.dev.yml` with no
dependency from any application code.

1. **Setup PostgresSaver** for thread checkpoints (app-db, `DATABASE_URL`)
2. **Setup PostgresStore** for long-term memory (same app-db connection)
3. **Run migrations** (`checkpointer.setup()`, `store.setup()`) to create tables
4. **Define namespace structure** for user preferences, saved queries, corrections
5. **Add cleanup job** to prevent checkpoint table bloat

### Phase 3: Frontend Chat UI (Week 5)

1. **Add ChatPanel component** (Mantine)
2. **Add Plotly.js rendering** (`react-plotly.js` with `plotly.js-basic-dist-min` partial bundle)
3. **Implement thread selector** for multiple chats
4. **SSE streaming integration**

### Phase 4: Testing & Refinement (Week 6-7)

1. Test Tier 1 coverage: verify library functions handle known query patterns correctly
2. Test Tier 2 escalation: agent writes and debugs novel queries end-to-end
3. Tune memory update triggers (when to save Tier 2 results back to library)
4. Add few-shot examples from successful Tier 2 sessions
5. User feedback loop integration

---

## 7. Docker Compose Changes

```yaml
# Add to docker-compose.yml

  cliproxyapi:
    image: ghcr.io/router-for-me/cli-proxy-api:latest
    ports:
      - "${BIND_ADDRESS:-127.0.0.1}:8080:8080"
    volumes:
      - ./resource/cliproxy/config.yaml:/app/config.yaml
      - ./resource/cliproxy/auths:/app/auths
      - ./resource/cliproxy/logs:/app/logs
    restart: unless-stopped
```

Cube is not added to `docker-compose.yml`. If developer schema exploration via
Cube's browser UI is useful, add it to a separate `docker-compose.dev.yml` that
is never started in production and has no dependency from application code.

---

## 8. Success Criteria

| Criterion | Metric |
|-----------|--------|
| **Tier 1 coverage** | Library functions handle 70%+ of real research requests without Tier 2 escalation |
| **Tier 1 accuracy** | 95%+ correct on first call for library-covered queries |
| **Tier 2 success rate** | Agent resolves novel queries within 2 iterations (write → error → fix) 80%+ of the time |
| **Funnel output** | 100% of complex queries return a stepwise funnel |
| **Memory recall** | Agent references relevant past Tier 2 results in 90%+ of follow-up sessions |
| **Job management** | 100% success for start/pause/resume/cancel |
| **Visualization** | 95%+ of charts render correctly on first generation |
| **Response latency** | < 3s for Tier 1 queries, < 20s for Tier 2 complex queries |

---

## 9. Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Query library doesn't cover a common pattern | Agent escalates to Tier 2; successful Tier 2 result gets promoted to library |
| LLM writes SQL with wrong table/column names in Tier 2 | `get_schema_info` tool + schema discovery queries run first; validate before execute |
| LLM generates psycopg-unsafe LIKE patterns | Tier 2 execution sandbox catches errors; known gotcha documented in system prompt |
| Novel temporal logic fails multiple retries | Agent falls back to Python post-processing on fetched rows; documented in guide §4.6 |
| `scoping_count` returns 0 incorrectly | Verify with `schema_discovery` + direct COUNT(*) before concluding data doesn't exist |
| Long conversations exceed context | Summarize result sets (counts + subject_ids, not raw rows); archive to long-term memory |
| OAuth tokens expire | CLIProxyAPI handles refresh automatically |
| Query library drifts from schema as DB evolves | Library functions use `schema_discovery` at startup; parameterized not hardcoded |

---

## Appendix A: Sample Agent System Prompt

```
You are NILS Research Assistant. Load nils-agent-guide.md as your full operational
reference. This prompt is a minimal bootstrap — the guide is authoritative.

Thread ID: {thread_id}
User: {user_id}

At start: read user memory (preferences, corrections, recent saved queries).
```

*Note: The full system prompt is the content of `nils-agent-guide.md`, not this
stub. This appendix shows the minimal bootstrap wrapper — the guide covers
capabilities, tool usage, SQL patterns, and behavioral rules.*

---

## Appendix B: Example Conversation Flow

**User**: "Show me MS patients with late onset who have at least 5 sessions with EDSS"

**Agent thinking**:
1. Read memory: no prior saved pattern for this exact query
2. `run_query("scoping_count", {"has_edss": True})` → feasible, proceed
3. Closest Tier 1 function: `clinical_proximity` combined with `cohort_demographics` won't fully express "≥5 sessions" — escalate to Tier 2

**Agent action**: Tier 2 — `execute_sql`
```sql
WITH ms_late_onset AS (
    SELECT s.subject_id
    FROM subject s
    JOIN subject_diseases sd ON s.subject_id = sd.subject_id AND sd.is_active = 1
    JOIN event onset_e ON sd.onset_event_id = onset_e.event_id
    WHERE (onset_e.event_date - s.birth_date) / 365.25 >= 45
),
sessions_with_edss AS (
    SELECT st.subject_id, st.study_id, nm.numeric_value AS edss
    FROM ms_late_onset m
    JOIN study st ON m.subject_id = st.subject_id
    JOIN numeric_measures nm ON nm.subject_id = st.subject_id
    JOIN event e ON nm.event_id = e.event_id
    JOIN observation_types ot ON nm.observation_type_id = ot.observation_type_id AND ot.name = 'EDSS'
    WHERE ABS(e.event_date - st.study_date) <= 91
)
SELECT subject_id, COUNT(DISTINCT study_id) AS n_sessions, AVG(edss) AS mean_edss
FROM sessions_with_edss
GROUP BY subject_id
HAVING COUNT(DISTINCT study_id) >= 5
ORDER BY n_sessions DESC
```

**Agent response**: "Found 47 late-onset MS patients with 5+ sessions that have EDSS within ±3 months. Would you like me to visualize their EDSS progression over time?"

**User**: "Yes, show me a chart"

**Agent action**: `create_visualization(chart_type="line_progression", data=result, ...)`

**User**: "Make the lines thicker and use a blue color scheme"

**Agent action**: `modify_visualization(patch={"data": [{"line": {"width": 3}}], "layout": {"colorway": ["#08306b", "#2171b5", "#6baed6"]}})`

**Agent memory update**:
```python
write_memory(path=f"user/{user_id}/preferences/chart_style",
             content={"line_width": 3, "color_scheme": "blues"})
```

---

---

## 10. Lessons Learned from Query Sessions

This section documents key insights from extensive query sessions that should inform agent behavior.

### 10.0 Core Principle: Database-Agnostic Design

The agent must work with **any database state** without hardcoded assumptions:

1. **Discover, don't assume**: Query the database to learn available cohorts, measures, classifications, treatments
2. **Validate before use**: Check that user-mentioned values actually exist in the database
3. **Parameterize patterns**: SQL templates should use parameters, not hardcoded values
4. **Cache dynamically**: Store discovered schema info in session state, refresh when needed
5. **Report what exists**: If user asks for something that doesn't exist, explain what IS available

```python
# Example: Agent discovers available cohorts at session start
async def discover_schema(db):
    return {
        "cohorts": await db.fetch("SELECT name FROM cohort"),
        "observation_types": await db.fetch("SELECT name, observation_type_id FROM observation_types"),
        "field_strengths": await db.fetch("SELECT DISTINCT magnetic_field_strength FROM mri_series_details"),
        "bases": await db.fetch("SELECT DISTINCT base FROM series_classification_cache WHERE base IS NOT NULL"),
        "disease_types": await db.fetch("SELECT type_name FROM disease_types"),
    }
```

### 10.1 Query Complexity Patterns Observed

Our sessions revealed several categories of query complexity:

| Category | Example | Key Challenge |
|----------|---------|---------------|
| **Multi-criteria filtering** | "3D T1 + 3D FLAIR, ≤1.5mm³, 3T, EDSS ±6mo" | 7+ table joins, multiple constraints |
| **Temporal proximity** | "EDSS within ±6 months of MRI" | `ABS(event_date - study_date) <= 183` |
| **Same-scanner matching** | "Same manufacturer + model across sessions" | Grouping by scanner fingerprint |
| **Interval counting** | "How many subject-interval pairs?" | Formula: `n × (n-1) / 2` |
| **Longitudinal subsets** | "Subjects followed from diagnosis" | First MRI within 1 year of diagnosis_event |

### 10.2 Critical Domain Terminology

The agent MUST understand these domain-specific terms:

| User Says | Agent Must Translate To | Notes |
|-----------|------------------------|-------|
| "Session" | `study` table (one scanner visit) | Universal term |
| "3D T1" | `base='T1w' AND mr_acquisition_type='3D'` | Verify `base` values exist in DB |
| "3D FLAIR" | `base='T2w' AND modifier_csv LIKE '%FLAIR%' AND mr_acquisition_type='3D'` | Check actual modifier patterns |
| "Voxel size ≤Xmm³" | `pixel_spacing_x × pixel_spacing_y × slice_thickness <= X` | X is user-specified |
| "Same scanner" | Same `manufacturer` AND `manufacturer_model_name` | May need clarification |
| "Same sequence" | Same `technique` AND `stack_orientation` | Clarify: for which modality? |
| "[Measure] within ±N months" | `ABS(event_date - study_date) <= N*30` | Measure from observation_types |
| "From diagnosis" | First MRI within threshold of `diagnosis_event_id → event.event_date` | Ask user for threshold |
| "Interval" vs "Session" | Intervals = pairwise combinations: `n × (n-1) / 2` | Always explain equivalence |
| "Interval group" | Sessions grouped by scanner+sequence for one subject | Define grouping criteria |

**Important**: The agent should verify terminology against actual database values before querying. For example, check what `base` values exist before assuming 'T1w' is valid.

### 10.3 Cohort-Specific Identifiers

**Critical learning**: Different cohorts may use different identifier systems.

The agent must:
1. **Discover** which identifier columns exist: `subject.subject_code`, `subject_other_identifiers.other_identifier`, etc.
2. **Detect** cohort context from user query
3. **Query** the database to determine which identifier is populated for that cohort
4. **Use** the appropriate identifier dynamically

```sql
-- Example: Check which identifier system a cohort uses
SELECT 
    c.name as cohort,
    COUNT(DISTINCT s.subject_code) as has_subject_code,
    COUNT(DISTINCT soi.other_identifier) as has_other_identifier
FROM cohort c
JOIN subject_cohorts sc ON c.cohort_id = sc.cohort_id
JOIN subject s ON sc.subject_id = s.subject_id
LEFT JOIN subject_other_identifiers soi ON s.subject_id = soi.subject_id
GROUP BY c.name;
```

The agent should NOT hardcode cohort-identifier mappings but discover them at runtime.

### 10.4 Clarification Decision Framework

**ASK the user when:**
- Multiple valid interpretations exist (e.g., "same sequence" = T1 only? or both T1 and FLAIR?)
- Domain context doesn't resolve ambiguity
- Getting it wrong would be costly (re-running expensive queries)

**DON'T ASK when:**
- Domain terminology has one clear meaning
- User provided explicit prior clarification
- Can investigate schema to find the answer

### 10.5 Query Refinement Patterns

Observed iterative refinement patterns:

1. **Broadening → Narrowing**: Start with "any FLAIR" → refine to "3D FLAIR only"
2. **Adding constraints**: Base query → add "same scanner" → add "same sequence"
3. **Splitting by dimension**: Combined results → split by field strength (1.5T vs 3T)
4. **Subset analysis**: All subjects → only those "from diagnosis" → only those with follow-up

### 10.6 Visualization Requirements

Key learnings for chart generation:

| Requirement | Solution |
|-------------|----------|
| **Sankey layout consistency** | Use `list(set([...]))` for node ordering to preserve Plotly's automatic layout |
| **Show raw numbers** | Add counts to node labels: `f'{treatment} ({count})'` |
| **Session ↔ Interval equivalence** | Always include "Sessions" column alongside "Intervals" in tables |
| **Only show bins with data** | Use categorical x-axis, don't show empty bins |
| **Bar labels** | Use `texttemplate='%{y}'` and `textposition='outside'` |

### 10.7 SQL Pattern Library

Successful patterns the agent should reuse. These are **templates** - actual values should be discovered from the database.

```sql
-- Voxel volume calculation (pixel_spacing is stored as 'X\\Y' string)
(SPLIT_PART(i.pixel_spacing, E'\\\\', 1)::float * 
 SPLIT_PART(i.pixel_spacing, E'\\\\', 2)::float * 
 s.slice_thickness) <= :voxel_threshold  -- User-specified threshold

-- Same scanner within session (two modalities from same station)
modality1.station_name = modality2.station_name 
OR (modality1.station_name IS NULL AND modality2.station_name IS NULL)

-- Same scanner across sessions (for interval groups)
GROUP BY subject_id, manufacturer, manufacturer_model_name, technique, orientation
HAVING COUNT(DISTINCT study_id) >= :min_sessions  -- User-specified minimum

-- Clinical measure within ±N months of MRI
JOIN event e ON subject_id = e.subject_id 
  AND e.observation_type_id = :measure_type_id  -- Lookup from observation_types
  AND ABS(e.event_date - study_date) <= :days_threshold

-- Diagnosis date lookup (via event linkage)
JOIN subject_diseases sd ON subject_id = sd.subject_id AND sd.is_active = 1
JOIN event diag_e ON sd.diagnosis_event_id = diag_e.event_id

-- First MRI within threshold of diagnosis
WHERE ABS(first_mri_date - diagnosis_date) <= :days_threshold

-- Interval calculation (pairwise combinations)
(COUNT(DISTINCT study_id) * (COUNT(DISTINCT study_id) - 1)) / 2 as n_intervals

-- Sequential transitions (treatment, disease type, etc.)
WITH ranked AS (
    SELECT subject_id, value, event_date,
           ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY event_date) as rn
    FROM source_table
)
SELECT r1.value as from_value, r2.value as to_value, COUNT(*)
FROM ranked r1
JOIN ranked r2 ON r1.subject_id = r2.subject_id AND r2.rn = r1.rn + 1
WHERE r1.value != r2.value
GROUP BY 1, 2
ORDER BY COUNT(*) DESC

-- Cohort filtering pattern
JOIN subject_cohorts sc ON subject_id = sc.subject_id
JOIN cohort c ON sc.cohort_id = c.cohort_id AND c.name = :cohort_name
```

**Note**: The `:parameter` placeholders indicate values that should be discovered from context or user input, never hardcoded.

### 10.8 State Management Insights

For the agent's conversation state:

| State Element | Purpose | When to Update |
|---------------|---------|----------------|
| **Current cohort** | Determines identifier type, filters | When user mentions cohort name |
| **Current query criteria** | Accumulates constraints | Each refinement step |
| **Last result set** | For follow-up questions ("for those N...") | After each successful query |
| **Last result count** | Reference for "those N subjects" | After each successful query |
| **Active visualizations** | For "make it like before" requests | After each chart generation |
| **Clarified terms** | User corrections to terminology | When user corrects agent |
| **Discovered schema** | Cached cohorts, measures, classifications | On first query, refresh periodically |

**Stateful References**: The agent must track numeric results so when user says "for those 445 subjects" or "of the 105 from diagnosis", it knows exactly which subset to query.

### 10.9 Error Recovery Patterns

Error recovery is a two-phase process: **diagnosis first, then fix**. Never blindly retry.

| Error Type | Diagnosis Step | Recovery Action |
|------------|---------------|-----------------|
| Wrong join path | `SELECT column_name FROM information_schema.columns WHERE table_name=...` | Find the correct FK chain; draw the path explicitly |
| Empty result set | Check each JOIN individually; print row count after each table added | Find which join kills the rows; check for NULL mismatches |
| Parameterized query LIKE failure | Test with hardcoded string first; check escaping | `%FLAIR%` → `%%FLAIR%%` in psycopg2/psycopg3 |
| Visualization subplot type error | Read the error: "Trace type X not compatible with subplot type Y" | Pass `specs=[[{"type":"pie"},...]]` to `make_subplots` |
| Missing data | Query the specific column for NULLs, check a sample of raw rows | Report what exists; suggest relaxing constraints |
| Cohort identifier mismatch | Check `subject_other_identifiers` table; verify which column has data for this cohort | Switch to correct identifier table |
| Manufacturer string mismatch | `SELECT DISTINCT manufacturer FROM study` | Add explicit normalization map covering all variants |
| Unexpectedly small result | Decompose: run each filter step independently and count (see 11.11) | Fix the step that over-filters |

**Concrete error recovery steps:**

```python
# Step 1: isolate — run each filter independently
print(count_without_filter_A)   # baseline
print(count_with_filter_A)      # did A kill too many?
print(count_with_filter_B)      # did B kill too many?

# Step 2: inspect sample rows at the failing join
SELECT * FROM table_A LIMIT 5;
SELECT * FROM table_B WHERE join_key = (SELECT join_key FROM table_A LIMIT 1);

# Step 3: check for type mismatches on the join column
SELECT pg_typeof(a.col), pg_typeof(b.col) FROM ...

# Step 4: check for NULL propagation
SELECT COUNT(*) FILTER (WHERE join_column IS NULL) FROM table;
```

### 10.10 The "Surprised? Investigate" Heuristic

**When a result looks unexpectedly small or large, do not accept it — diagnose it.**

This is one of the most important agent behaviors. An unexpected number is a signal, not a final answer.

**Triggers for automatic investigation:**
- A tier/category that should have many entries has very few (e.g., Tier 3 has only 2 sessions)
- A filter step drops >50% of the population unexpectedly
- A count matches a previous known number exactly (possible deduplication error)
- A count is 0 where some data was expected

**Investigation protocol:**

```
Unexpected result observed
        ↓
1. DECOMPOSE: break the query into parts; what does each filter remove?
   SELECT COUNT(*) at each filter boundary (not just the end)

2. CHARACTERIZE: what IS in the small set? Sample 5 rows.
   What is NOT included? Find representative excluded rows.

3. HYPOTHESIZE: propose the most likely explanation
   (wrong join, wrong field, data sparsity, legitimate filter)

4. VERIFY: run a targeted query that confirms or refutes the hypothesis

5. CONCLUDE: either fix the query, or confirm the result is correct
   and explain WHY it is small
```

**Real example from C4 analysis:**
- Observed: Tier 3 (MP2RAGE) = only 2 sessions despite 517 total sessions with MP2RAGE + FLAIR
- Investigation: checked field strength distribution → `SELECT magnetic_field_strength, COUNT(*) ... GROUP BY 1`
- Finding: 516 of 517 are 7T (Siemens Terra.X); only 1.5T/3T are valid for C4 coverage
- Conclusion: result is correct; 7T protocols have brain-only FOV by design

**This saved us from incorrectly widening the filter to include 7T sessions.**

### 10.11 Recommended Agent Workflow

The workflow has six phases. The most important structural insight is that **DISCOVER runs in parallel with PLAN** — you do not need to finish exploration before writing the query plan.

```
1. UNDERSTAND
   - Parse user request: cohort, sequence types, constraints, output format
   - Identify ambiguous terms (see 11.4 for when to ask vs. investigate)
   - Reference past clarifications from memory
   - If ambiguous: use structured multi-choice clarification (see 11.15)

2. PLAN + DISCOVER (simultaneously, not sequentially)
   ┌──────────────────────────────────┬──────────────────────────────────┐
   │ PLAN (write todo list)           │ DISCOVER (parallel queries)      │
   │ - List required tables           │ - SELECT DISTINCT on assumed cols │
   │ - Identify join path             │ - Verify column names exist      │
   │ - List filter conditions in order│ - Check row counts per table     │
   │ - Plan output format             │ - Sample 5 rows from key tables  │
   │ - Decompose multi-step logic     │ → All run at the same time       │
   └──────────────────────────────────┴──────────────────────────────────┘
   
   The todo list is a live contract:
   - Created at PLAN time with all steps as [pending]
   - Exactly ONE step is [in_progress] at any moment
   - Steps are marked [completed] only AFTER verification, not after writing code
   - New steps are added when unexpected sub-problems are discovered

3. EXECUTE (with stepwise funnel)
   - Use CTEs to make each filter step independently countable
   - Count subjects/sessions after EACH step, not just the final result
   - Calculate: count lost and percentage lost from previous step
   - Identify the biggest bottleneck step
   - Validate results: null checks, sanity-check extremes
   - Read any file before editing it (never edit blind)
   - If a result is unexpected → invoke "Surprised? Investigate" (11.10)

4. VERIFY (before reporting to user)
   - Run the final query one more time with LIMIT to spot-check rows
   - Cross-check: does the total equal sum of parts?
   - Check for off-by-one in date windows (use inclusive bounds explicitly)
   - For Python scripts: run the script, confirm it exits without error
   - For visualizations: confirm chart renders with correct axis labels

5. PRESENT
   - Show stepwise funnel FIRST — it is the most informative artifact
   - Label the biggest drop clearly (e.g., "◄ BIGGEST BOTTLENECK")
   - Show final summary table with absolute counts
   - State all assumptions explicitly (e.g., "DMT initiation = new real drug ≠ previous real drug")
   - Note any caveats (e.g., "1 subject has 2 qualifying episodes")
   - Offer visualization if appropriate

6. ITERATE
   - Preserve accumulated constraints; never restart from scratch
   - When user says "those N subjects", match to the exact prior result set
   - When relaxing criteria, show updated funnel to compare before/after
   - When tightening criteria, explain what is additionally lost and why
```

**Parallel exploration example — what "simultaneously" means in practice:**

Instead of:
```
→ Check if column 'technique' exists
→ Check distinct values of 'technique'
→ Check if body_part is nullable
→ Count sessions with both T1 and FLAIR
```

Do all four at once in a single multi-query block:
```sql
-- Query block sent in one round-trip
SELECT column_name FROM information_schema.columns WHERE table_name='series_classification_cache';
SELECT DISTINCT technique FROM series_classification_cache WHERE technique IS NOT NULL LIMIT 20;
SELECT COUNT(*) FILTER (WHERE body_part IS NULL) FROM series_classification_cache;
SELECT COUNT(DISTINCT study_id) FROM series WHERE ... T1 ... INTERSECT SELECT ... FLAIR ...;
```

This matters because each round-trip to the database has latency. Batching exploration queries dramatically reduces total wall time.

### 10.12 Multi-Tier Evidence Design

When ground truth is uncertain or incomplete, design a **tiered evidence hierarchy** rather than a single approach. Each tier should have an explicit confidence level and a clear non-overlap guarantee with higher tiers.

**Pattern:**

```
Tier 1 (highest confidence): Direct manual verification or gold-standard annotation
Tier 2 (high confidence):    Systematic heuristic with empirically validated thresholds
Tier 3 (moderate):           Indirect inference from correlated signals
```

**How to derive thresholds empirically (instead of assuming them):**

```python
# Step 1: Take a sample of confirmed-positive cases (Tier 1)
known_good = load_tier1_cases()

# Step 2: Measure the relevant signal in those cases
distribution = [measure_signal(case) for case in known_good]

# Step 3: Choose threshold that covers 95%+ of known-good cases
threshold = percentile(distribution, 5)   # conservative lower bound

# Step 4: Validate: check what the threshold would include/exclude
fp_rate = count_false_positives_at_threshold(threshold)
fn_rate = count_false_negatives_at_threshold(threshold)
```

**Real example — C4 FOV thresholds:**
- Took 127 known-good Tier 1 sessions
- Measured: n_slices distribution (mode 160, range 120–190), fov_y (mode 240, range 230–250)
- Set threshold: n_slices ≥ 160 AND fov_y ≥ 230mm (covering 95%+ of Tier 1 cases)
- Validated by checking that no Tier 1 session was excluded by these thresholds

**When designing tiers, always verify:**
1. Each tier adds genuinely new sessions not covered by higher tiers
2. The confidence claim for each tier is empirically grounded, not assumed
3. A surprisingly small tier is a signal to investigate (see 11.10), not a problem to fix by lowering standards

### 10.13 Tool and Approach Selection

The agent should reason explicitly about which tool/approach to use for a given sub-task. Not every problem needs SQL; not every post-processing step should stay in SQL.

**Decision framework:**

| Sub-task | Preferred approach | Reason |
|----------|--------------------|--------|
| Filter, join, count, group by | SQL (CTE or single query) | Database does this efficiently at scale |
| Multi-step logic with branching (e.g., "pick BL closest to DMT date") | Python post-processing on fetched rows | SQL gets unwieldy; Python is clearer |
| String normalization (manufacturer aliases) | Python dict map at fetch time | Brittle to do in SQL; easy to audit |
| Statistical summary (mean, SD, median) | Python `statistics` module or SQL | SQL for large sets; Python for already-fetched data |
| Threshold derivation from known-good samples | Python (array analysis) | Needs percentile/histogram inspection |
| Visualization | Plotly in Python script | Generates both HTML (interactive) and PNG |
| Complex deduplication (e.g., distinct EDSS event_ids across timepoints) | Python with explicit sets | Much easier to track state than SQL |

**When to move logic from SQL to Python:**
- The query requires a stateful loop (e.g., "for each DMT initiation, find the nearest BL session, then from that BL find the nearest M6-18 session")
- The query requires previously-computed intermediate values as inputs to subsequent filters
- The query has "pick the best of N candidates by priority" logic (technique priority: MPRAGE > MP2RAGE > FLASH)
- The SQL is becoming a 100+ line monolith that is hard to debug

**When to keep it in SQL:**
- Pure filtering and aggregation with no branching logic
- When the result set is too large to fetch into memory
- When intermediate steps don't need Python inspection

### 10.14 Structured Clarification (When to Ask and How)

When clarification is needed, use **structured multi-choice questions**, never open-ended ones. Open-ended questions stall the conversation; multiple-choice keeps it moving.

**Pattern:**
```
Agent: I need to clarify two things before I run this:

1. "Same scanner" — which sequences should match?
   A) T1 and FLAIR must be from the same scanner
   B) Only T1 needs same scanner; FLAIR can differ
   C) Just tell me — any session where both are present (any scanner)

2. "Within 6 months" — does this mean:
   A) EDSS within 6 months before the session only
   B) Within 6 months in either direction (±6 months)
```

**When to ask (see also 11.4):**

| Situation | Ask or investigate? |
|-----------|-------------------|
| Term has 2+ equally plausible definitions | ASK |
| Getting it wrong redefines the research question | ASK |
| Can resolve by querying the schema | INVESTIGATE |
| User has clarified this before in current session | NEVER ASK AGAIN — use state |
| Minor implementation detail with no semantic impact | DECIDE and state your assumption |

**When not to ask:**
- Don't ask about field names that exist in the schema — look them up
- Don't ask what values a column contains — query `SELECT DISTINCT`
- Don't ask if data exists — query `COUNT(*)`
- Don't ask about things you can determine empirically in seconds

**State the assumption instead:**
If a decision is minor and empirically determinable, make the call and say so:
> "I'm using ±6 months (183 days) as the EDSS proximity window — let me know if you want a different threshold."

This is faster than asking, and the user can correct it if needed.

### 10.15 Context and Result State Management

The agent must track prior results explicitly so that follow-up references like "for those 421 subjects" or "of the 46 episodes" are unambiguous.

**What to track in thread state:**

```python
class QueryState:
    last_result_count: int           # "for those N" references this
    last_result_subject_ids: list    # used to filter subsequent queries
    last_result_label: str           # "DMT episodes with BL+M6-18+M18-24"
    last_funnel: list[FunnelStep]    # for "what if I relax step 3?"
    accumulated_filters: dict        # all constraints added so far
    active_chart_spec: dict          # current Plotly figure (for patches)
    clarified_terms: dict            # "same scanner" → "T1 and FLAIR same station"
```

**Context efficiency rules:**
- Do not re-fetch data already in context — reference it by variable name
- When a query returns >1000 rows, store only a summary and the subject_ids list, not raw rows
- When running follow-up queries on a subset, pass subject_ids as a parameter (`WHERE subject_id = ANY(:ids)`) rather than re-applying all original filters
- Summarize large intermediate results rather than printing every row

**The "N from previous step" pattern:**

When user says "for those 191 subjects, show age and EDSS distribution":
```python
# CORRECT: filter by cached IDs from previous result
subject_ids = state.last_result_subject_ids  # [1032, 2847, ...]
cur.execute(
    "SELECT ... WHERE subject_id = ANY(%s)",
    (subject_ids,)
)

# WRONG: re-apply all the original filters from scratch
# (fragile, slower, may give different results if data changed)
```

### 10.16 Normalization and String Matching Vigilance

String fields in DICOM-derived metadata are notoriously inconsistent. Always expect variation and normalize explicitly.

**Known problematic fields:**

| Field | Variants seen in NILS | Normalized value |
|-------|-----------------------|-----------------|
| `manufacturer` | `SIEMENS`, `Siemens`, `Siemens Healthineers` | `SIEMENS` |
| `manufacturer` | `Philips Medical Systems`, `Philips Healthcare`, `Philips` | `PHILIPS` |
| `manufacturer_model_name` | `Prisma_fit`, `MAGNETOM Prisma Fit` | `Prisma_fit` |
| `modifier_csv` FLAIR filter | `%FLAIR%` | use `%%FLAIR%%` in psycopg parameterized queries |
| Treatment names | `Ingen behandling`, `Annan` (non-DMT entries mixed with real drugs) | Explicit exclusion list |

**The scanner normalization lesson:** In the pregnancy cohort analysis, 2 subjects were missing from results because `SIEMENS` and `Siemens Healthineers` were treated as different scanners. Adding explicit normalization maps recovered them.

**Rule:** Before any grouping or matching on a string field, run `SELECT DISTINCT field FROM table` and inspect the full list. Build an explicit normalization map, even if it seems redundant.

**psycopg escaping rule:** In parameterized queries with `%s` placeholders, literal `%` in LIKE patterns must be doubled: `LIKE '%%FLAIR%%'` not `LIKE '%FLAIR%'`.

**Stepwise Funnel Example**:
```
| Step | Filter                              | Subjects | Sessions | Lost      |
|------|-------------------------------------|----------|----------|-----------|
| 1    | STOPMS with SPMS transition         | 267      | -        | -         |
| 2    | With MRI 1-6yr before conversion    | 199      | 655      | -68 (25%) |
| 3    | With any FLAIR sequence             | 198      | 556      | -1 (1%)   |
| 4    | With FLAIR ≤2mm slice thickness     | 76       | 170      | -122 (62%)| ◄ BIGGEST DROP
| 5    | With baseline EDSS ±1yr of MRI      | 43       | 93       | -33 (43%) |
| 6    | With follow-up EDSS 5±1yr after MRI | 41       | 79       | -2 (5%)   |
```

This funnel immediately shows that Step 4 (FLAIR resolution) loses 62% of subjects - valuable information for users considering criteria relaxation.

---

## 11. Example Query Workflows

### 11.1 Complete Workflow: Longitudinal MRI Analysis

**User request**: "How many [COHORT] subjects have 3D T1 + 3D FLAIR at 3T with EDSS? And how many have same scanner across sessions?"

**Agent workflow**:

1. **Parse requirements**:
   - Cohort: [detected from query]
   - Sequences: 3D T1 + 3D FLAIR (both ≤1.5mm³)
   - Field strength: 3T
   - Clinical: EDSS within ±6 months
   - Two queries: (1) all eligible, (2) same scanner subset

2. **Discover cohort metadata**:
   ```sql
   -- Check cohort exists and get subject count
   SELECT c.name, COUNT(DISTINCT sc.subject_id) 
   FROM cohort c 
   JOIN subject_cohorts sc ON c.cohort_id = sc.cohort_id
   WHERE c.name ILIKE '%user_input%'
   GROUP BY c.name;
   ```

3. **Execute Query 1** (all eligible):
   - Join required tables (subject_cohorts, study, series, etc.)
   - Apply all filters
   - Return session and subject counts

4. **Execute Query 2** (same scanner + sequence):
   - Group by: manufacturer, model, technique, orientation
   - Calculate interval groups and counts

5. **Present results**:
   - Summary table with both query results
   - Session distribution histogram
   - Interval distribution histogram
   - Note any assumptions made (e.g., "Same sequence applies to T1 only")

6. **Follow-up**: "For those N, show demographics"
   - Agent maintains reference to previous result set
   - Runs sex, disease type, treatment queries for that subset

### 11.2 Iterative Refinement Example

**Turn 1**: "Show me [cohort] subjects with T1 and FLAIR"
- Agent discovers cohort, returns sessions across all field strengths

**Turn 2**: "Only 3T"
- Agent adds filter: `magnetic_field_strength = 3`
- Returns: subset at 3T

**Turn 3**: "Only 3D acquisitions"
- Agent adds: `mr_acquisition_type = '3D'` for both T1 and FLAIR
- Returns: smaller subset

**Turn 4**: "With voxel size under 1.5"
- Agent adds: voxel volume calculation ≤ 1.5
- Returns: final filtered set

**Key**: Agent accumulates constraints, doesn't restart from scratch.

### 11.3 Provenance Filtering: Avoiding ProjectionDerived Stacks

**Critical Lesson Learned**: When selecting MRI stacks for export or analysis, ALWAYS filter out `ProjectionDerived` provenance to get the original acquisition data.

#### The Problem

In NILS, a single 3D acquisition (e.g., 3D FLAIR) can generate multiple stacks in the database:

| Stack ID | Provenance | Slice Thickness | Description |
|----------|------------|-----------------|-------------|
| 79567 | `RawRecon` | 1mm | **Original 3D acquisition** (WANT THIS) |
| 79569 | `ProjectionDerived` | 2mm | MIP/MPR reformat |
| 79563 | `ProjectionDerived` | 2mm | Another projection view |
| 79559 | `ProjectionDerived` | 2mm | Another projection view |

`ProjectionDerived` stacks are Maximum Intensity Projections (MIP) or Multi-Planar Reconstructions (MPR) created by the scanner from the original 3D data. They are useful for clinical viewing but **not suitable for quantitative analysis** because:

1. They have lower resolution (thick slices reformatted from thin 3D data)
2. They may have different orientations (axial projections from sagittal acquisition)
3. They don't represent the actual acquired data

#### The Bug

A query without provenance filtering might select a `ProjectionDerived` stack instead of `RawRecon`:

```sql
-- WRONG: May select ProjectionDerived stack
SELECT DISTINCT ON (study_id) series_stack_id
FROM series_classification_cache scc
JOIN series s ON scc.series_id = s.series_id
WHERE scc.base = 'T2w' AND scc.modifier_csv LIKE '%FLAIR%'
  AND s.slice_thickness <= 2.0
ORDER BY study_id, s.slice_thickness;  -- Arbitrary selection!
```

This happened in 13 sessions where both `RawRecon` and `ProjectionDerived` FLAIR stacks qualified.

#### The Fix

ALWAYS exclude `ProjectionDerived` when selecting stacks for data export:

```sql
-- CORRECT: Exclude ProjectionDerived, prefer smallest slice thickness
SELECT DISTINCT ON (study_id) series_stack_id
FROM series_classification_cache scc
JOIN series s ON scc.series_id = s.series_id
WHERE scc.base = 'T2w' AND scc.modifier_csv LIKE '%FLAIR%'
  AND s.slice_thickness <= 2.0
  AND (scc.provenance != 'ProjectionDerived' OR scc.provenance IS NULL)
ORDER BY study_id, s.slice_thickness ASC, series_stack_id ASC;
```

#### When to Apply This Filter

| Use Case | Filter ProjectionDerived? |
|----------|---------------------------|
| Exporting data for analysis | **YES** - always want original data |
| Counting available sequences | Usually yes, unless specifically counting projections |
| Finding sessions with a modality | Yes - projections come with the original |
| QC/visual inspection | Maybe no - projections can be useful for review |

#### Provenance Values in NILS

| Provenance | Meaning | Include in Export? |
|------------|---------|-------------------|
| `RawRecon` | Original scanner reconstruction | **YES** |
| `NULL` | Default (same as RawRecon) | **YES** |
| `SyMRI` | Synthetic MRI from qMRI maps | Yes (if needed) |
| `SWIRecon` | SWI processing output | Yes (if needed) |
| `BOLDRecon` | BOLD processing output | Yes (if needed) |
| `ProjectionDerived` | MIP/MPR from 3D | **NO** |

#### Standard Filter Pattern

Use this pattern in all stack selection queries for export:

```sql
AND (scc.provenance != 'ProjectionDerived' OR scc.provenance IS NULL)
```

Or equivalently:

```sql
AND COALESCE(scc.provenance, 'RawRecon') != 'ProjectionDerived'
```

---

*Document version: 2.3*
*Created: 2026-02-12*
*Updated: 2026-02-23 (Replaced Vega-Lite with Plotly.js; added Section 3.3: tradeoff mitigations)*
*Updated: 2026-03-09 (Added §10.9–§10.16 agent behavioral patterns; §4 Semantic Layer / Cube Views section removed; sections renumbered 4–11; §3.1, §5.1, §5.6, §6.1, §7, §8, §9 updated)*
*Author: NILS Development Team*
