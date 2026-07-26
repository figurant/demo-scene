from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement_audit_sql_demo import pipeline
from procurement_audit_sql_demo.config import load_runtime_config
from procurement_audit_sql_demo.fixture_loader import build_fixture
from procurement_audit_sql_demo.pipeline import CORE_RELATIONS, run_pipeline
from procurement_audit_sql_demo.vane_functions import (
    AuditFactContractError,
    stable_json,
    validate_audit_fact_for_role_json,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = PROJECT_ROOT / "fixtures/expert-score-anomaly"


def test_materialize_relation_uses_runner_backed_relation_write():
    expected = pa.table({"value": [1, 2]})
    calls = []

    class Relation:
        def write_parquet(self, path):
            calls.append(Path(path))
            pq.write_table(expected, path)

        def to_arrow_table(self):
            raise AssertionError("direct DuckDB materialization used")

    actual = pipeline.materialize_relation(Relation())

    assert len(calls) == 1
    assert actual.equals(expected)


def _source_and_store():
    fixture = build_fixture(FIXTURE_DIR)

    class Store:
        objects = {
            (item.bucket, item.object_key): item.value for item in fixture.objects
        }

        def get_bytes(self, bucket, object_key):
            return self.objects[(bucket, object_key)]

    return fixture.source, Store()


def _fixed_ai_table() -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "project_id": "PRJ-2026-001",
                "file_id": "EVD-REC-001",
                "raw_response": stable_json(
                    {
                        "document_type": "recommendation_record",
                        "expert_id": "EXP-001",
                        "supplier_name": "景维自动化有限公司",
                        "recommended": True,
                        "participated": None,
                        "recused": None,
                        "evidence_quote": "推荐供应商：景维自动化有限公司",
                        "confidence": 0.96,
                    }
                ),
            },
            {
                "project_id": "PRJ-2026-001",
                "file_id": "EVD-MIN-001",
                "raw_response": stable_json(
                    {
                        "document_type": "committee_minutes",
                        "expert_id": "EXP-001",
                        "supplier_name": None,
                        "recommended": None,
                        "participated": True,
                        "recused": False,
                        "evidence_quote": "参加评审：是；是否回避：否",
                        "confidence": 0.95,
                    }
                ),
            },
        ]
    )


def _fixed_ai_attempt_table(*, invalid_recommendation: bool = False) -> pa.Table:
    role_by_file = {
        "EVD-REC-001": "expert_recommendation",
        "EVD-MIN-001": "committee_minutes",
    }
    rows = []
    for row in _fixed_ai_table().to_pylist():
        rows.append(
            {
                **row,
                "role": role_by_file[row["file_id"]],
                "image_bytes": b"\x89PNG fixture image",
                "raw_response": (
                    "not-json"
                    if invalid_recommendation
                    and row["file_id"] == "EVD-REC-001"
                    else row["raw_response"]
                ),
            }
        )
    return pa.Table.from_pylist(rows)


def test_source_loader_reads_postgres_snapshot(monkeypatch):
    config = load_runtime_config(PROJECT_ROOT / "runtime.yml")
    expected, _store = _source_and_store()
    events = []

    class Connection:
        def __enter__(self):
            events.append("postgres:enter")
            return self

        def __exit__(self, *_args):
            events.append("postgres:exit")
            return False

    monkeypatch.setattr(
        pipeline,
        "connect_postgres",
        lambda pg_config: events.append(pg_config.raw_schema) or Connection(),
    )
    monkeypatch.setattr(
        pipeline,
        "read_source_rows",
        lambda _connection, _config: (
            expected.project.to_pylist(),
            expected.suppliers.to_pylist(),
            expected.scores.to_pylist(),
            expected.evidence.to_pylist(),
        ),
    )

    actual = pipeline.read_source_bundle(config)

    assert actual.project.to_pylist() == expected.project.to_pylist()
    assert actual.evidence.to_pylist() == expected.evidence.to_pylist()
    assert events == ["procurement_audit_raw", "postgres:enter", "postgres:exit"]


def test_pipeline_runs_eight_relations_and_publishes(tmp_path, monkeypatch):
    config = replace(
        load_runtime_config(PROJECT_ROOT / "runtime.yml"),
        output_dir=tmp_path / "output",
    )
    events = []
    ocr_calls = []
    source, _store = _source_and_store()

    def configure_runner(*, runner):
        events.append(f"configure:{runner}")

    def attach_functions(connection, _config, _local_ocr_results):
        events.append("attach_functions")

        def evidence_ocr(_bucket, object_key):
            ocr_calls.append(object_key)
            return stable_json(
                {
                    "status": "success",
                    "full_text": "fixture OCR text",
                    "mean_confidence": 0.95,
                    "text_line_count": 1,
                    "error": None,
                }
            )

        connection.create_function(
            "evidence_ocr_json",
            evidence_ocr,
            ["VARCHAR", "VARCHAR"],
            "VARCHAR",
        )
        connection.create_function(
            "try_validate_audit_fact_for_role_json",
            lambda raw_response, role: _try_validate_for_role(
                raw_response,
                role,
            ),
            ["VARCHAR", "VARCHAR"],
            "VARCHAR",
        )
        connection.create_function(
            "validate_audit_fact_for_role_json",
            validate_audit_fact_for_role_json,
            ["VARCHAR", "VARCHAR"],
            "VARCHAR",
        )

    execute_runner_sql_file = pipeline._execute_runner_sql_file

    def execute_runner_stage(
        connection,
        runner_connection,
        path,
        **kwargs,
    ):
        if path == pipeline.EVIDENCE_AI_ATTEMPT_1_STAGE:
            rows = connection.sql(
                "select file_id from int_evidence_ai_inputs order by file_id"
            ).fetchall()
            events.append(f"ai:{len(rows)}")
            assert {row[0] for row in rows} == {"EVD-REC-001", "EVD-MIN-001"}
            pipeline.register_or_replace_table(
                connection,
                "int_evidence_ai_attempt_1",
                _fixed_ai_attempt_table(invalid_recommendation=True),
            )
            return
        if path == pipeline.EVIDENCE_AI_ATTEMPT_2_STAGE:
            retry_rows = connection.sql(
                "select file_id, image_bytes "
                "from int_evidence_ai_retry_inputs"
            ).fetchall()
            events.append(f"retry:{len(retry_rows)}")
            assert retry_rows == [
                ("EVD-REC-001", b"\x89PNG fixture image")
            ]
            pipeline.register_or_replace_table(
                connection,
                "int_evidence_ai_attempt_2",
                _fixed_ai_attempt_table().slice(0, 1),
            )
            return
        execute_runner_sql_file(
            connection,
            runner_connection,
            path,
            **kwargs,
        )

    monkeypatch.setattr(
        pipeline,
        "_execute_runner_sql_file",
        execute_runner_stage,
    )

    result = run_pipeline(
        config,
        configure_runner=configure_runner,
        runtime_probe=lambda _config: None,
        runtime_function_attacher=attach_functions,
        ai_health_probe=lambda _config: events.append("health"),
        source_loader=lambda _config: source,
        local_ocr_result_builder=lambda _source, _config: {},
        relation_materializer=lambda relation: relation.to_arrow_table(),
    )

    assert events == [
        "configure:local",
        "attach_functions",
        "health",
        "ai:2",
        "retry:1",
    ]
    assert len(ocr_calls) == 2
    assert all(object_key.endswith(".png") for object_key in ocr_calls)
    assert result.executed_relations == CORE_RELATIONS
    assert result.finding_count == 3
    assert result.summary_count == 1
    assert result.summary["status"] == "review_required"
    assert result.summary["winner_without_flagged_expert"] == "SUP-ZJ-002"
    assert len(result.findings) == 3
    assert len(
        (tmp_path / "output/audit_findings.jsonl").read_text(encoding="utf-8").splitlines()
    ) == 3
    assert json.loads(
        (tmp_path / "output/audit_summary.jsonl").read_text(encoding="utf-8")
    )["flagged_expert_id"] == "EXP-001"


def test_pipeline_fails_before_ai_when_ocr_coverage_is_incomplete(
    tmp_path,
):
    config = replace(
        load_runtime_config(PROJECT_ROOT / "runtime.yml"),
        output_dir=tmp_path / "output",
    )
    source, _store = _source_and_store()
    health_calls = []

    def attach_functions(connection, _config, _local_ocr_results):
        def evidence_ocr(_bucket, object_key):
            if object_key.endswith("expert_recommendation.png"):
                return stable_json(
                    {
                        "status": "success",
                        "full_text": "专家编号 EXP-001；推荐供应商 景维自动化有限公司",
                        "mean_confidence": 0.95,
                        "text_line_count": 1,
                        "error": None,
                    }
                )
            return stable_json(
                {
                    "status": "unreadable",
                    "full_text": "",
                    "mean_confidence": 0.0,
                    "text_line_count": 0,
                    "error": "no_text_detected",
                }
            )

        connection.create_function(
            "evidence_ocr_json",
            evidence_ocr,
            ["VARCHAR", "VARCHAR"],
            "VARCHAR",
        )
    with pytest.raises(Exception, match="coverage must match"):
        run_pipeline(
            config,
            configure_runner=lambda **_kwargs: None,
            runtime_probe=lambda _config: None,
            runtime_function_attacher=attach_functions,
            ai_health_probe=lambda _config: health_calls.append("health"),
            source_loader=lambda _config: source,
            local_ocr_result_builder=lambda _source, _config: {},
            relation_materializer=lambda relation: relation.to_arrow_table(),
        )

    assert health_calls == []
    assert not (tmp_path / "output/audit_findings.jsonl").exists()
    assert not (tmp_path / "output/audit_summary.jsonl").exists()


def _try_validate_for_role(raw_response: str, role: str) -> str:
    try:
        return validate_audit_fact_for_role_json(raw_response, role)
    except AuditFactContractError:
        return ""
