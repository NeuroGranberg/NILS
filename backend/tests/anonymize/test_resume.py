from __future__ import annotations

from pathlib import Path

import importlib
import sys

import pytest


@pytest.fixture
def anonymize_modules(tmp_path, monkeypatch):
    db_file = tmp_path / "audit.sqlite"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_file}")

    import db.config as db_config
    import db.session as db_session
    import jobs.models as jobs_models

    db_config.get_settings.cache_clear()
    db_config = importlib.reload(db_config)
    db_session = importlib.reload(db_session)

    jobs_models.Base.registry.dispose()
    jobs_models.Base.metadata.clear()

    for module_name in ["anonymize.store", "anonymize.core"]:
        if module_name in sys.modules:
            del sys.modules[module_name]

    store = importlib.import_module("anonymize.store")
    core = importlib.import_module("anonymize.core")
    return store, core


def test_study_audit_helpers_use_sqlite(anonymize_modules):
    store, _ = anonymize_modules

    assert store.study_audit_exists("1.2.3") is False
    store.mark_study_audit_complete("1.2.3", leaf_rel_path="leaf", cohort_name="ALS")
    assert store.study_audit_exists("1.2.3") is True


def test_leaf_summary_upsert(anonymize_modules):
    store, _ = anonymize_modules
    from db.session import session_scope

    audit_payload = {
        "anchor_rel_path": "sub-01/file1.dcm",
        "tags": [
            {
                "tag": "(0010,0010)",
                "tag_name": "PatientName",
                "action": "removed",
                "old_value": "Foo",
                "new_value": "",
            }
        ],
    }

    store.record_leaf_audit_summary(
        "1.2.3",
        cohort_name="ALS",
        leaf_rel_path="sub-01",
        files_total=10,
        files_written=8,
        files_reused=2,
        files_with_errors=1,
        patient_id_original="PAT001",
        patient_id_updated="ALS0001",
        errors=["x"],
        audit_payload=audit_payload,
    )

    with session_scope() as session:
        row = (
            session.query(store.AnonymizeLeafSummary)
            .filter(store.AnonymizeLeafSummary.study_instance_uid == "1.2.3")
            .one()
        )
        assert row.files_total == 10
        assert row.summary["patient_id_original"] == "PAT001"
        assert row.summary["audit"]["tags"][0]["tag"] == "(0010,0010)"

    # Upsert should overwrite counts
    store.record_leaf_audit_summary(
        "1.2.3",
        cohort_name="ALS",
        leaf_rel_path="sub-01",
        files_total=11,
        files_written=9,
        files_reused=1,
        files_with_errors=0,
        patient_id_original="PAT001",
        patient_id_updated="ALS0001",
        errors=[],
        audit_payload={"anchor_rel_path": "sub-01/file2.dcm", "tags": []},
    )

    with session_scope() as session:
        row = (
            session.query(store.AnonymizeLeafSummary)
            .filter(store.AnonymizeLeafSummary.study_instance_uid == "1.2.3")
            .one()
        )
        assert row.files_total == 11
        assert row.files_with_errors == 0
        assert row.summary["audit"]["anchor_rel_path"] == "sub-01/file2.dcm"


def test_leaf_summary_loader_returns_one_row_per_leaf(anonymize_modules):
    store, _ = anonymize_modules

    store.record_leaf_audit_summary(
        "1.2.3",
        cohort_name="ALS",
        leaf_rel_path="sub-01",
        files_total=1,
        files_written=1,
        files_reused=0,
        files_with_errors=0,
        patient_id_original=None,
        patient_id_updated=None,
        errors=[],
        audit_payload={
            "anchor_rel_path": "sub-01/file1.dcm",
            "tags": [
                {"tag": "(0010,0010)", "tag_name": "PatientName", "action": "removed", "old_value": "Foo", "new_value": ""}
            ],
        },
    )

    rows = store.load_leaf_summaries_for_cohort("ALS")
    assert len(rows) == 1
    assert rows[0]["summary"]["audit"]["tags"][0]["tag_name"] == "PatientName"



def test_determine_leaf_mode_behaviour(tmp_path, monkeypatch, anonymize_modules):
    _, core = anonymize_modules

    source_root = tmp_path / "derivatives" / "dcm-original"
    output_root = tmp_path / "derivatives" / "dcm-raw"
    leaf_dir = source_root / "sub-01"
    leaf_dir.mkdir(parents=True)
    output_root.mkdir(parents=True)
    file_path = leaf_dir / "file.dcm"
    file_path.write_bytes(b"")

    options = core._Options(
        source_root=source_root,
        output_root=output_root,
        scrub_tags=[],
        exclude_tags=set(),
        anonymize_patient_id=False,
        map_timepoints=False,
        preserve_uids=True,
        rename_patient_folders=False,
        resume=True,
        audit_resume_per_leaf=True,
        cohort_name="ALS",
    )

    pid_strategy = core.IDStrategy()
    leaf_states: dict[Path, core._LeafState] = {}
    study_cache = {"1.2.3": True}

    monkeypatch.setattr(core, "_light_read_uid_and_pid", lambda path: ("1.2.3", "PAT001"))
    monkeypatch.setattr(core, "_outputs_exist_for_path", lambda *args, **kwargs: True)

    resume_metrics = core._ResumeMetrics()

    mode, state = core._determine_leaf_process_mode(
        file_path,
        options,
        pid_strategy,
        leaf_states,
        study_cache,
        resume_metrics,
    )
    assert mode is core._LeafProcessMode.SKIP
    assert state is not None and state.audit_done is True
    assert resume_metrics.leaves_skipped == 1

    # Force output-only path when outputs missing
    monkeypatch.setattr(core, "_outputs_exist_for_path", lambda *args, **kwargs: False)
    mode, state = core._determine_leaf_process_mode(
        file_path,
        options,
        pid_strategy,
        leaf_states,
        study_cache,
        resume_metrics,
    )
    assert mode is core._LeafProcessMode.OUTPUT_ONLY

    # When audit is not complete we should request full processing
    options.resume = True
    leaf_states.clear()
    study_cache = {"1.2.3": False}
    monkeypatch.setattr(core, "_outputs_exist_for_path", lambda *args, **kwargs: False)
    resume_metrics = core._ResumeMetrics()
    mode, state = core._determine_leaf_process_mode(
        file_path,
        options,
        pid_strategy,
        leaf_states,
        study_cache,
        resume_metrics,
    )
    assert mode is core._LeafProcessMode.FULL
    assert state is not None and state.processed_for_audit is True


def test_finalize_leaf_states_marks_only_success(monkeypatch, anonymize_modules):
    _, core = anonymize_modules

    options = core._Options(
        source_root=Path("/tmp/src"),
        output_root=Path("/tmp/out"),
        scrub_tags=[],
        exclude_tags=set(),
        anonymize_patient_id=False,
        map_timepoints=False,
        preserve_uids=True,
        rename_patient_folders=False,
        resume=True,
        audit_resume_per_leaf=True,
        cohort_name="ALS",
    )

    leaf_states = {
        Path("/tmp/src/sub-01"): core._LeafState(
            leaf_rel_path="sub-01",
            study_uid="1.2.3",
            processed_for_audit=True,
            files_seen=5,
            files_written=4,
            files_existing=1,
            patient_id_original="PAT001",
            patient_id_updated="ALS001",
            anchor_rel_path="sub-01/file1.dcm",
            audit_tags={
                "(0010,0010)": {
                    "tag": "(0010,0010)",
                    "tag_name": "PN",
                    "action": "removed",
                    "old_value": "Foo",
                    "new_value": "",
                }
            },
        ),
        Path("/tmp/src/sub-02"): core._LeafState(
            leaf_rel_path="sub-02",
            study_uid="1.2.4",
            processed_for_audit=True,
            had_error=True,
        ),
    }

    called: list[tuple[str, dict]] = []
    summaries: list[tuple[str, dict]] = []

    def _fake_mark(uid, **kwargs):
        called.append((uid, kwargs))

    def _fake_summary(uid, **kwargs):
        summaries.append((uid, kwargs))

    resume_metrics = core._ResumeMetrics()
    monkeypatch.setattr(core, "mark_study_audit_complete", _fake_mark)
    monkeypatch.setattr(core, "record_leaf_audit_summary", _fake_summary)

    completed = core._finalize_leaf_audit_states(leaf_states, options, resume_metrics)

    assert called == [("1.2.3", {"leaf_rel_path": "sub-01", "cohort_name": "ALS"})]
    assert summaries[0][0] == "1.2.3"
    assert summaries[0][1]["files_total"] == 5
    assert summaries[0][1]["audit_payload"]["anchor_rel_path"] == "sub-01/file1.dcm"
    assert summaries[0][1]["audit_payload"]["tags"][0]["tag_name"] == "PN"
    assert resume_metrics.leaves_completed == 1
    assert completed == 1


def test_events_from_summaries_recreate_audit_rows(anonymize_modules):
    _, core = anonymize_modules

    summaries = [
        {
            "study_uid": "1.2.3",
            "leaf_rel_path": "sub-01",
            "summary": {
                "audit": {
                    "anchor_rel_path": "sub-01/file1.dcm",
                    "tags": [
                        {
                            "tag": "(0010,0010)",
                            "tag_name": "PatientName",
                            "action": "removed",
                            "old_value": "Foo",
                            "new_value": "",
                        }
                    ],
                }
            },
        }
    ]

    events = core._events_from_summaries(summaries)
    assert len(events) == 1
    assert events[0]["rel_path"] == "sub-01/file1.dcm"
    assert events[0]["tag_name"] == "PatientName"
