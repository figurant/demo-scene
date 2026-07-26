# Procurement Audit Operational Runbook

[Back to the use case](../README.md) · [简体中文](runbook.zh-CN.md)

This runbook contains the exact environment, installation, model-service, configuration, execution, and troubleshooting contracts for the procurement audit demo.

## Verified environment

| Item | Verified value |
| --- | --- |
| Operating system | Ubuntu 24.04 x86_64, glibc 2.39 |
| Python | CPython 3.12 |
| Vane | `vane-ai==0.1.0a1` |
| PostgreSQL | `127.0.0.1:5432`, database `vane_insight` |
| MinIO | `127.0.0.1:9000`, HTTP |
| Model service | `Qwen2.5-VL-3B-Instruct` on a local NVIDIA GPU |

This SQL-AI branch requires the image-capable Vane build that exposes `ai_prompt(VARCHAR, BLOB, STRUCT)`. Its package metadata remains `vane-ai==0.1.0a1`, so the launcher also pins the DuckDB engine/source identifiers and runs a SQL capability probe. A wheel with the same package version but without that overload is rejected.

Install the project-side Ubuntu tools:

```bash
sudo apt update
sudo apt install -y git curl python3.12 python3.12-venv
```

## Install from a clean checkout

Run all project commands from the `procurement-compliance-audit` directory.

### 1. Create the virtual environment

```bash
cd procurement-compliance-audit
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### 2. Install Vane from public PyPI

```bash
python -m pip install vane-ai
```

Use this command only when the published wheel contains the image-capable build. During local Vane development, activate the prebuilt Vane worktree environment instead, then install the demo dependencies into that active environment. The launcher rejects a same-version build whose engine identifiers or SQL image overload do not match.

### 3. Install the demo

```bash
python -m pip install -r requirements.txt
python -m pip check
```

This installs the source in editable mode; `pyproject.toml` is authoritative:

| Dependency | Purpose |
| --- | --- |
| `vane-ai==0.1.0a1` | Vane APIs, custom DuckDB, and workers |
| `openai==2.45.0` | OpenAI-compatible Qwen client |
| `psycopg` | Read and initialize PostgreSQL raw tables |
| `minio` | Read and initialize raw material objects in MinIO |
| `rapidocr`, `onnxruntime` | CPU OCR: driver-owned on Local, stateful Actor on Ray |
| `pillow` | Image reads |
| `pyarrow` | Relation/Python data boundary |
| `pyyaml` | Strict `runtime.yml` loading |
| `pytest` | Fast tests |

`pip check` must report no broken requirements.

## Prepare PostgreSQL and MinIO

The checked-in `runtime.yml` uses these loopback contracts by default:

| Service | Default contract |
| --- | --- |
| PostgreSQL | `postgresql://vane_insight:***@127.0.0.1:5432/vane_insight` |
| MinIO | access/secret `vaneinsight` / `vaneinsight_dev_password`, `127.0.0.1:9000`, HTTP |

You may reuse existing services or install them from their official documentation. The `fixture` command creates the four raw tables and MinIO bucket and refreshes synthetic data; it does not create the servers, PostgreSQL database/role, MinIO process, or access key.

After configuring the services, initialize the sources independently with:

```bash
python scripts/run_demo.py fixture
```

The runtime pipeline never reads `fixtures/`; that directory is only a reproducible synthetic seed for the `fixture` command.

## Prepare the local Qwen service

The model server must use a separate environment. Follow the shared [Qwen2.5-VL setup guide](../../docs/local-qwen-service.md) for NVIDIA, vLLM, model download, startup, and troubleshooting.

Verify the service contract:

```bash
curl -fsS -o /dev/null -w 'health HTTP %{http_code}\n' \
  http://127.0.0.1:8001/health
curl -fsS -H 'Authorization: Bearer dummy' \
  http://127.0.0.1:8001/v1/models | python -m json.tool
```

Expected: health is HTTP 200 and the model list contains `Qwen2.5-VL-3B-Instruct`. The standalone guide also verifies a real image request to `/v1/chat/completions`.

## Run and verify

The launcher exposes three commands:

```bash
python scripts/run_demo.py fixture
python scripts/run_demo.py run
python scripts/run_demo.py e2e
```

- `fixture` writes one project, three suppliers, twelve scores, and two evidence locators to PostgreSQL, plus two PNG objects to MinIO.
- `run` probes PostgreSQL/MinIO, reads every input from those services, and executes real OCR, Qwen, and the SQL DAG.
- `e2e` runs `fixture -> run` in order.

For a first run:

```bash
python scripts/run_demo.py e2e
```

Expected terminal output:

```text
Result: review_required | 3 findings | flagged expert EXP-001
Winner recalculation: SUP-JW-001 -> SUP-ZJ-002
```

The command writes only:

```text
output/audit_findings.jsonl  # 3 rows
output/audit_summary.jsonl   # 1 row
```

There is no AI mock fallback. The command fails explicitly if Qwen is unavailable, an image is unreadable, AI JSON violates its contract, or the Vane runtime is incompatible.

Run deterministic tests without starting Qwen:

```bash
python -m pytest tests/fast -q
```

## Synthetic seed and source contract

`fixtures/expert-score-anomaly/` contains exactly four synthetic seed files, read only by the `fixture` command:

| File | Grain | Purpose |
| --- | --- | --- |
| `project.json` | one procurement project | Suppliers, MinIO object keys, original winner, and rule thresholds |
| `expert_scores.csv` | expert × supplier, 12 rows | Scores from four experts for three suppliers |
| `expert_recommendation.png` | one image | `EXP-001` recommends Jingwei before the tender |
| `committee_minutes.png` | one image | `EXP-001` participates and is marked as not recused |

All names, companies, and documents are synthetic. The score matrix guarantees that:

- `SUP-JW-001` has the highest average with all experts;
- `EXP-001` gives Jingwei 98 while other experts average 80, an 18-point deviation;
- `SUP-ZJ-002` ranks first after removing `EXP-001`.

At runtime, the authoritative sources are the PostgreSQL `projects`, `suppliers`, `expert_scores`, and `evidence_files` tables plus the MinIO bucket `procurement-compliance-audit-fixtures`. `evidence_files.bucket/object_key` is the trusted PostgreSQL-to-MinIO locator. The pipeline, OCR implementation, and AI request builder never read local paths.

## Relation contracts

| Relation | Materialization | Grain | Purpose |
| --- | --- | --- | --- |
| `stg_scores` | view | expert × supplier | Type, score, and supplier contracts |
| `stg_evidence_images` | view | evidence image | Trusted locator and role |
| `int_evidence_ocr` | table | evidence image | Typed output from the SQL-called stateful OCR expression |
| `int_evidence_ai` | table | evidence image | Raw Qwen JSON response |
| `int_conflict_facts` | view | evidence image | Validated recommendation, participation, and recusal facts |
| `int_score_metrics` | view | project × conflict signal | Peer average, score delta, and both rankings |
| `audit_findings` | table | finding | Three deterministic rules |
| `audit_summary` | table | project | Project-level audit status |

`int_evidence_ai_inputs.sql` builds one trusted, role-specific prompt row per usable OCR result, applies the OCR confidence threshold, and fails unless every trusted evidence image is covered. `int_evidence_ai_attempt_1.sql` loads the corresponding MinIO image BLOB and invokes multimodal `ai_prompt`. A direct try-validator selects JSON/role-contract failures for `int_evidence_ai_attempt_2.sql`; `int_evidence_ai.sql` then keeps the valid first response or the single retry response. Each response remains bound to `project_id/file_id`, and the final strict validator rejects malformed JSON or a document type that disagrees with the trusted role.

Use `queries.sql` to inspect all eight relations in the same connection:

```sql
select * from int_score_metrics;
select * from audit_findings order by rule_id;
select * from audit_summary;
```

## AI response and decision contracts

Qwen returns only document type, expert ID, supplier, recommendation, participation, recusal, source evidence text, and confidence. It does not decide whether a violation occurred.

The local service may wrap JSON in one complete code fence. The attached stateless UDF normalizes only that outer fence and rejects surrounding prose, missing or unknown fields, invalid types, and placeholder evidence.

Both trusted images must have successful, non-empty OCR above the configured threshold. Incomplete OCR coverage fails before Qwen and publishes nothing. A first JSON/role-contract failure is retried once with the same image and a reinforced prompt; an invalid retry fails strict validation. When both responses are valid but either AI confidence is below `0.75`, SQL emits no findings and marks the summary `insufficient_evidence`.

The deterministic findings are:

1. `EXP-001-conflict-not-recused`
2. `EXP-002-score-bias`
3. `EXP-003-award-impact`

## Output contract

With the normal fixture, `audit_findings.jsonl` contains exactly three rows and `audit_summary.jsonl` exactly one. With insufficient evidence, findings contains zero rows and summary contains one row with status `insufficient_evidence`.

Before writing, the pipeline validates fields, primary keys, enums, counts, and evidence references. Each file is atomically replaced through a temporary file in the same directory.

## Runtime configuration

The checked-in `runtime.yml` defines:

| Setting | Default |
| --- | --- |
| Runner | `local` |
| PostgreSQL raw tables | `procurement_audit_raw.projects`, `suppliers`, `expert_scores`, `evidence_files` |
| MinIO | `127.0.0.1:9000`, bucket `procurement-compliance-audit-fixtures` |
| Output directory | `output` |
| OCR | RapidOCR on CPU, minimum confidence `0.60` |
| AI | OpenAI provider at `http://127.0.0.1:8001/v1`; model `Qwen2.5-VL-3B-Instruct`; concurrency `1`; timeout `120` seconds |

The checked-in configuration uses:

```yaml
runner: local
```

The checked-in value is `runner: local`; change it to `runner: ray` for the distributed path. The image-capable local Vane build uses the same SQL relation contracts in both modes.

On Local, the pipeline creates one `EvidenceOcrActor` implementation on the driver, processes every trusted evidence locator once, and attaches the immutable results as `evidence_ocr_json(bucket, object_key)`.

On Ray, `EvidenceOcrActor` is attached as the stateful `evidence_ocr_json(bucket, object_key)` expression. The OCR engine initializes lazily inside its isolated Actor worker. The launcher sets `VANE_UDF_UNREGISTER_TIMEOUT_MS=60000` unless the operator supplied another value, giving native Ray OCR workers enough time to shut down cleanly.

In both modes, `int_evidence_ocr_udf.sql` calls the same expression once per image and `int_evidence_ocr.sql` parses the same materialized JSON. `int_evidence_ai_inputs.sql` constructs prompts and enforces complete OCR coverage; the first attempt loads each image BLOB, and the SQL-selected retry reuses those exact staged bytes for its second `ai_prompt` call. Final response validation keeps the `int_conflict_validation_udf.sql` then `int_conflict_facts.sql` shape. Driver-local inputs are staged as temporary Parquet files and Runner results are registered in the driver's DuckDB catalog for the next pure SQL node.

## Troubleshooting

| Symptom | Resolution |
| --- | --- |
| `No matching distribution found for vane-ai` | This demo requires CPython 3.12; for the published wheel, confirm x86_64 Linux and glibc 2.28 or newer, then run `python -m pip install vane-ai` against public PyPI |
| Python/Vane/DuckDB version mismatch | Reactivate `.venv` and reinstall from public PyPI; the launcher reports the current interpreter, prefix, and expected/actual values |
| Ray cannot allocate memory or satisfy query demand | Stop stale Ray processes, free host memory, or connect to a Ray cluster with enough CPU, heap, and object-store capacity; this real OCR flow was validated with 8 CPUs and a 2 GiB object store |
| A direct dependency is missing | Run `python -m pip install -r requirements.txt` and `python -m pip check` |
| PostgreSQL connection, authentication, or table initialization failure | Check the DSN, database/role, port, and schema/table read/write permissions |
| MinIO connection, authentication, or object-read failure | Check endpoint, HTTP/TLS, access key, and bucket list/read/write/delete permissions |
| Qwen health or image request fails | Use the [Qwen guide](../../docs/local-qwen-service.md) to check port, driver, OOM, model name, and proxy settings |
| Output does not contain three findings | Inspect the terminal error and Qwen response; the default fixture's OCR or AI confidence did not meet its threshold |

## Exact runtime identifiers

| Component | Required identifier |
| --- | --- |
| Vane distribution metadata (`vane-ai`) | `0.1.0a1` |
| `vane.__version__` | `0.1.0a1` |
| DuckDB Python package | `0.1.0a1` |
| DuckDB engine | `v1.6.0-dev2` |
| DuckDB source revision | `b1e6e66d56` |
| OpenAI Python client | `2.45.0` |

The required API surface includes `vane.func`, `vane.cls`, `vane.attach_function`, `vane.configure`, and `duckdb.ray_cxx`. The launcher then executes `select ai_prompt(NULL, NULL::BLOB, NULL)` to prove the image overload is present. Any identity or capability mismatch fails startup instead of silently falling back to ordinary DuckDB.

## Data, credentials, and privacy

- Do not commit real procurement records, evidence documents, personal data, production credentials, model weights, or generated output.
- Fixture names, companies, scores, and documents are synthetic.
- `runtime.yml` contains loopback demo configuration only.
