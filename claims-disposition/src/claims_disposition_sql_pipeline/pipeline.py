"""Explicit orchestration for the standalone claims disposition SQL DAG."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
from typing import Any, Callable, Mapping, Sequence

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import vane

from .config import DEFAULT_CONFIG_PATH, RuntimeConfig, load_runtime_config
from .minio_store import MinioStore
from .output_writer import replace_output_rows
from .pg import connect_postgres, probe_postgres, read_claim_rows
from .photo_ai import DAMAGE_SYSTEM_MESSAGE, probe_qwen
from .vane_udfs import (
    DocumentOcrActor,
    build_minio_udfs,
    stable_json,
    stateless_udf_specs,
)


SQL_ROOT = Path(__file__).resolve().parent / "sql"
SQL_STAGES = (
    SQL_ROOT / "staging/stg_claims.sql",
    SQL_ROOT / "staging/stg_claim_materials.sql",
    SQL_ROOT / "staging/stg_run_config.sql",
)
MATERIAL_INPUT_STAGE = SQL_ROOT / "intermediate/int_claim_material_inputs.sql"
OBJECT_PROBE_UDF_STAGE = SQL_ROOT / "intermediate/int_claim_object_probe_udf.sql"
OBJECT_FACT_STAGE = SQL_ROOT / "intermediate/int_claim_object_facts.sql"
OBJECT_HASH_UDF_STAGE = SQL_ROOT / "intermediate/int_claim_object_hash_udf.sql"
PHOTO_QUALITY_UDF_STAGE = SQL_ROOT / "intermediate/int_claim_photo_quality_udf.sql"
DOCUMENT_OCR_UDF_STAGE = SQL_ROOT / "intermediate/int_claim_document_ocr_udf.sql"
OBJECT_FACT_UDF_STAGES = (
    OBJECT_HASH_UDF_STAGE,
    PHOTO_QUALITY_UDF_STAGE,
    DOCUMENT_OCR_UDF_STAGE,
)
DOCUMENT_FIELDS_UDF_STAGE = SQL_ROOT / "intermediate/int_claim_document_fields_udf.sql"
DOCUMENT_QUALITY_INPUT_STAGE = SQL_ROOT / "intermediate/int_claim_document_quality_inputs.sql"
DOCUMENT_QUALITY_UDF_STAGE = SQL_ROOT / "intermediate/int_claim_document_quality_udf.sql"
MATERIAL_FACT_STAGE = SQL_ROOT / "intermediate/int_claim_material_facts.sql"
PHOTO_AI_INPUT_STAGE = SQL_ROOT / "intermediate/int_claim_photo_ai_inputs.sql"
PHOTO_AI_STAGE = SQL_ROOT / "intermediate/int_claim_photo_ai.sql"
DAMAGE_VALIDATION_INPUT_STAGE = SQL_ROOT / "intermediate/int_claim_damage_validation_inputs.sql"
DAMAGE_VALIDATION_UDF_STAGE = SQL_ROOT / "intermediate/int_claim_damage_validation_udf.sql"
DAMAGE_FACT_STAGE = SQL_ROOT / "intermediate/int_claim_damage_facts.sql"
SQL_FINAL_STAGES = (
    SQL_ROOT / "intermediate/int_claim_decision_facts.sql",
    SQL_ROOT / "marts/claim_disposition.sql",
)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CREATE_RELATION_AS = re.compile(
    r"create\s+or\s+replace\s+(?:table|view)\s+"
    r"(?P<target>[A-Za-z_][A-Za-z0-9_]*)\s+as\s+(?P<query>.+)",
    re.IGNORECASE | re.DOTALL,
)


class RuntimeConnectionError(ConnectionError):
    """Raised when one or more required storage services are unavailable."""


class RunnerWorkspace:
    """Stage driver-local Arrow data as Parquet scans for the active Runner."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.counter = 0

    def stage_table(self, name: str, table: pa.Table) -> Path:
        """Persist Arrow input as a Parquet scan for the configured Runner."""

        self.counter += 1
        path = self.root / f"{self.counter:03d}-{_safe_identifier(name)}.parquet"
        pq.write_table(table, path)
        return path

    def relation_from_table(
        self,
        connection: Any,
        name: str,
        table: pa.Table,
    ) -> Any:
        """Expose a staged Arrow snapshot as a Vane relation."""

        path = self.stage_table(name, table)
        return connection.sql(f"select * from read_parquet({_sql_literal(path)})")


def build_run_config_row(
    config: RuntimeConfig,
    run_started_at: datetime,
) -> dict[str, Any]:
    """Build the single secret-free row consumed by SQL models."""

    if run_started_at.tzinfo is None or run_started_at.utcoffset() is None:
        raise ValueError("run_started_at must be timezone-aware")
    return {
        "runtime_config_version": config.version,
        "run_started_at": run_started_at.isoformat(),
        "ocr_engine": config.ocr.engine,
        "ocr_device": config.ocr.device,
        "required_fields_json": stable_json(list(config.ocr.required_fields)),
        "minimum_text_confidence": config.ocr.minimum_text_confidence,
        "minio_bucket": config.minio.bucket,
    }


def _arrow_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return stable_json(value)
    return value


def rows_to_arrow(rows: list[Mapping[str, Any]]) -> pa.Table:
    """Normalize JSON values and convert runtime rows to Arrow."""

    normalized = [
        {key: _arrow_value(value) for key, value in row.items()}
        for row in rows
    ]
    return pa.Table.from_pylist(normalized)


def read_claim_rows_with_json(config: RuntimeConfig) -> list[dict[str, Any]]:
    """Read the ordered PostgreSQL snapshot with stable JSON strings."""

    with connect_postgres(config.postgres) as connection:
        rows = read_claim_rows(connection, config.postgres)
    return [
        {key: _arrow_value(value) for key, value in dict(row).items()}
        for row in rows
    ]


def _safe_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid DuckDB identifier: {value!r}")
    return value


def _sql_literal(value: Path) -> str:
    return _sql_string(str(value))


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ai_sql_replacements(config: RuntimeConfig) -> dict[str, str]:
    """Render constant, credential-free SQL options required by ai_prompt."""

    return {
        "__AI_PROVIDER_SQL__": _sql_string(config.ai.provider),
        "__AI_MODEL_SQL__": _sql_string(config.ai.model),
        "__AI_BASE_URL_SQL__": _sql_string(config.ai.base_url),
        "__AI_TIMEOUT_SQL__": repr(config.ai.timeout_seconds),
        "__AI_CONCURRENCY_SQL__": str(config.ai.concurrency),
        "__AI_TEMPERATURE_SQL__": repr(config.ai.temperature),
        "__AI_MAX_TOKENS_SQL__": str(config.ai.max_tokens),
        "__AI_SYSTEM_MESSAGE_SQL__": _sql_string(DAMAGE_SYSTEM_MESSAGE),
    }


@contextmanager
def _openai_api_key(value: str):
    """Expose the configured credential to SQL AI actors without serializing it."""

    previous_api_key = os.environ.get("OPENAI_API_KEY")
    os.environ["OPENAI_API_KEY"] = value
    try:
        yield
    finally:
        if previous_api_key is None:
            os.environ.pop("OPENAI_API_KEY", None)
        else:
            os.environ["OPENAI_API_KEY"] = previous_api_key


def register_or_replace_table(
    connection: Any,
    target: str,
    table: pa.Table,
) -> None:
    """Replace a DuckDB table from an Arrow snapshot without name interpolation."""

    name = _safe_identifier(target)
    temporary = f"__{name}_arrow_input"
    try:
        connection.unregister(temporary)
    except Exception:
        pass
    connection.register(temporary, table)
    try:
        connection.execute(
            f"create or replace table {name} as select * from {temporary}"
        )
    finally:
        connection.unregister(temporary)


def materialize_relation(relation: Any) -> pa.Table:
    """Materialize a Relation through the active Vane Runner write API."""

    with TemporaryDirectory(prefix="claims-runner-result-") as root:
        path = Path(root) / "result.parquet"
        relation.write_parquet(str(path))
        return pq.read_table(path)


def probe_runtime(config: RuntimeConfig) -> None:
    """Report every unavailable storage service in one credential-free error."""

    failures: list[str] = []
    try:
        probe_postgres(config.postgres)
    except Exception as exc:
        failures.append(f"PostgreSQL ({config.postgres.raw_relation}): {exc}")
    try:
        MinioStore(config.minio).probe()
    except Exception as exc:
        failures.append(f"MinIO ({config.minio.endpoint}): {exc}")
    if failures:
        raise RuntimeConnectionError("; ".join(failures))


def attach_runtime_functions(connection: Any, config: RuntimeConfig) -> None:
    """Attach all SQL-callable Vane functions to the active connection."""

    for spec in stateless_udf_specs(build_minio_udfs(config.minio)):
        vane.attach_function(
            spec.function,
            connection=connection,
            alias=spec.alias,
            parameters=list(spec.parameters),
            replace=True,
        )
    if config.runner == "ray":
        vane.attach_function(
            DocumentOcrActor(config.minio),
            connection=connection,
            alias="document_ocr_json",
            parameters=["VARCHAR", "VARCHAR"],
            replace=True,
        )


def attach_local_document_ocr_lookup(
    runner_connection: Any,
    driver_connection: Any,
    config: RuntimeConfig,
) -> None:
    """Run native OCR on the driver and expose its immutable results as a task UDF."""

    locators = driver_connection.sql(
        "select distinct cast(bucket as varchar), cast(object_key as varchar) "
        "from int_claim_object_facts "
        "where object_exists "
        "and role = 'supporting_document' "
        "and media_type = 'image/png' "
        "order by 1, 2"
    ).fetchall()
    ocr = DocumentOcrActor(config.minio)
    results = {
        (bucket, object_key): ocr(bucket, object_key)
        for bucket, object_key in locators
    }

    def document_ocr_json(bucket: str, object_key: str) -> str:
        return results[(bucket, object_key)]

    vane.attach_function(
        document_ocr_json,
        connection=runner_connection,
        alias="document_ocr_json",
        parameters=["VARCHAR", "VARCHAR"],
        return_dtype="VARCHAR",
        replace=True,
    )


def _sql_stage_parts(
    path: Path,
    replacements: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    statement = path.read_text(encoding="utf-8")
    for marker, replacement in (replacements or {}).items():
        statement = statement.replace(marker, replacement)
    if "__AI_" in statement:
        raise RuntimeError(f"unresolved AI SQL option in stage: {path}")
    match = _CREATE_RELATION_AS.fullmatch(
        statement.strip().removesuffix(";").strip()
    )
    if match is None:
        raise RuntimeError(f"invalid SQL stage: {path}")
    return match.group("target"), match.group("query")


def _execute_runner_sql_file(
    connection: Any,
    runner_connection: Any,
    path: Path,
    *,
    workspace: RunnerWorkspace,
    source_relations: Sequence[str],
    materializer: Callable[[Any], pa.Table],
    replacements: Mapping[str, str] | None = None,
) -> None:
    """Run one SQL model through the active Vane Runner and register its result."""

    for source_name in source_relations:
        name = _safe_identifier(source_name)
        table = connection.sql(f"select * from {name}").to_arrow_table()
        workspace.relation_from_table(
            runner_connection,
            name,
            table,
        ).create_view(name, replace=True)
    target, query = _sql_stage_parts(path, replacements)
    register_or_replace_table(
        connection,
        target,
        materializer(runner_connection.sql(query)),
    )


def _create_empty_photo_ai(connection: Any) -> None:
    """Create the AI relation without scheduling ai_prompt for zero photos."""

    connection.execute(
        """
        create or replace table int_claim_photo_ai as
        select
          cast(claim_id as varchar) as claim_id,
          cast(file_id as varchar) as file_id,
          file_order,
          cast(photo_sha256 as varchar) as photo_sha256,
          cast(photo_quality_json as varchar) as photo_quality_json,
          cast(null as varchar) as raw_damage_response
        from int_claim_photo_ai_inputs
        where false
        """
    )


def _execute_sql_file(
    connection: Any,
    path: Path,
) -> None:
    try:
        statement = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"cannot read SQL stage {path}: {exc}") from exc
    connection.execute(statement)


def run_pipeline(
    config: RuntimeConfig,
) -> int:
    """Execute the complete DAG and atomically publish its validated mart."""

    # The configured backend changes execution only; the relation code is shared.
    vane.configure(runner=config.runner)
    # Fail before computation if either PostgreSQL or MinIO is unavailable.
    probe_runtime(config)
    run_started_at = datetime.now(timezone.utc)
    claim_rows = read_claim_rows_with_json(config)

    with TemporaryDirectory(
        prefix=f"claims-disposition-{config.runner}-"
    ) as workspace_root:
        workspace = RunnerWorkspace(Path(workspace_root))
        connection = duckdb.connect()
        runner_connection = duckdb.connect()
        try:
            # Register the PostgreSQL snapshot and secret-free run settings.
            register_or_replace_table(
                connection,
                "claims_runtime_claims",
                rows_to_arrow(claim_rows),
            )
            register_or_replace_table(
                connection,
                "claims_runtime_run_config",
                rows_to_arrow([build_run_config_row(config, run_started_at)]),
            )
            attach_runtime_functions(runner_connection, config)
            # Build the driver-local staging relations first.
            for sql_path in SQL_STAGES:
                _execute_sql_file(connection, sql_path)
            # Make every object-store/OCR call a direct Runner SQL projection.
            _execute_sql_file(connection, MATERIAL_INPUT_STAGE)
            _execute_runner_sql_file(
                connection,
                runner_connection,
                OBJECT_PROBE_UDF_STAGE,
                workspace=workspace,
                source_relations=("int_claim_material_inputs",),
                materializer=materialize_relation,
            )
            _execute_sql_file(connection, OBJECT_FACT_STAGE)
            for sql_path in OBJECT_FACT_UDF_STAGES:
                if config.runner == "local" and sql_path == DOCUMENT_OCR_UDF_STAGE:
                    attach_local_document_ocr_lookup(
                        runner_connection,
                        connection,
                        config,
                    )
                _execute_runner_sql_file(
                    connection,
                    runner_connection,
                    sql_path,
                    workspace=workspace,
                    source_relations=("int_claim_object_facts",),
                    materializer=materialize_relation,
                )
            _execute_runner_sql_file(
                connection,
                runner_connection,
                DOCUMENT_FIELDS_UDF_STAGE,
                workspace=workspace,
                source_relations=("int_claim_document_ocr_udf",),
                materializer=materialize_relation,
            )
            _execute_sql_file(connection, DOCUMENT_QUALITY_INPUT_STAGE)
            _execute_runner_sql_file(
                connection,
                runner_connection,
                DOCUMENT_QUALITY_UDF_STAGE,
                workspace=workspace,
                source_relations=("int_claim_document_quality_inputs",),
                materializer=materialize_relation,
            )
            # Aggregate the Runner outputs into the AI-ready claim contract.
            _execute_sql_file(connection, MATERIAL_FACT_STAGE)
            # Build one trusted SQL row per photo, then load verified image bytes
            # and call the multimodal model as one Runner SQL relation.
            _execute_sql_file(connection, PHOTO_AI_INPUT_STAGE)
            photo_ai_count = connection.execute(
                "select count(*) from int_claim_photo_ai_inputs"
            ).fetchone()[0]
            if photo_ai_count:
                probe_qwen(config.ai)
                with _openai_api_key(config.ai.api_key):
                    _execute_runner_sql_file(
                        connection,
                        runner_connection,
                        PHOTO_AI_STAGE,
                        workspace=workspace,
                        source_relations=("int_claim_photo_ai_inputs",),
                        materializer=materialize_relation,
                        replacements=_ai_sql_replacements(config),
                    )
            else:
                _create_empty_photo_ai(connection)
            # Bind AI output in SQL, validate it through one direct Runner UDF,
            # then keep classification, rules, and marts in pure SQL stages.
            _execute_sql_file(connection, DAMAGE_VALIDATION_INPUT_STAGE)
            _execute_runner_sql_file(
                connection,
                runner_connection,
                DAMAGE_VALIDATION_UDF_STAGE,
                workspace=workspace,
                source_relations=("int_claim_damage_validation_inputs",),
                materializer=materialize_relation,
            )
            _execute_sql_file(connection, DAMAGE_FACT_STAGE)
            for sql_path in SQL_FINAL_STAGES:
                _execute_sql_file(connection, sql_path)
            rows = connection.sql(
                "select * from claim_disposition order by claim_id"
            ).to_arrow_table().to_pylist()
        finally:
            runner_connection.close()
            connection.close()

    # Validate the output contract before replacing the PostgreSQL result table.
    return replace_output_rows(rows, config)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the standalone claims disposition SQL pipeline."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Static runtime YAML path.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = load_runtime_config(args.config)
        row_count = run_pipeline(config)
    except Exception as exc:
        print(f"pipeline failed: {exc}", file=sys.stderr)
        return 1
    print(f"published {row_count} claim dispositions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
