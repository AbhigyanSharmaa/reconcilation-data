# ARF - Automated Reconciliation Framework (WBS 1.4)

ARF is a Python/SQL tool that checks whether a migrated BigQuery table matches the legacy Databricks Delta table it came from. For every table it compares the **schema**, the **row count**, and the **numeric aggregates** (SUM, MIN, MAX, NULL counts). Optionally, **row hash validation** compares every row, so it finds rows that are missing, extra or changed. Every mismatch or failure goes to an **exceptions log**.

Each engine computes its own aggregates and returns one row of numbers, so full tables never leave Azure or GCP.

| WBS 1.4 task | Where it is built |
|---|---|
| Extraction connectors for Databricks Delta tables and BigQuery | `arf/connectors/databricks.py`, `arf/connectors/bigquery.py` (plus `sqlite.py` and `spark.py` for local testing) |
| Comparison modules: schema (names, types, nullability), row counts, numeric aggregations (SUM, MIN, MAX, NULL counts) | `arf/compare/schema.py`, `arf/compare/row_count.py`, `arf/compare/numeric.py` |
| Tolerance thresholds for floating-point precision and timestamp formatting | `arf/tolerance.py`, the `tolerance:` settings in YAML |
| Unit tests on sample source/target tables, plus automated exception logging | `tests/` (126 tests), `python -m arf demo`, `arf/sinks.py` |

## How it works

1. **Config** (`config/settings.yaml` + one YAML per table) says which tables to compare, with filters, column renames and tolerances.
2. **Connectors** read each table's schema from metadata. They then run `COUNT(*)` and one aggregate `SELECT` on each engine.
3. **Comparators** match the columns, apply the tolerances and mark each check PASS, WARN, FAIL, ERROR or SKIPPED.
4. **Sinks** write every result to `arf_results` and the problem rows to `arf_exceptions` (console, CSV and/or BigQuery). The exit code is 0 when the run passes and 1 when there is a FAIL or ERROR, so Airflow can stop the pipeline.

One table failing, or one connection failing, never stops the other tables.

---

## 1. Set up on your laptop (Windows + VS Code)

You need Python 3.10 or newer, Git, VS Code (with the Python extension) and the Google Cloud CLI.

```powershell
# in VS Code: File > Open Folder > arf-framework, then Terminal > New Terminal
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

If PowerShell blocks `activate`, run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once.
Then press Ctrl+Shift+P, choose **Python: Select Interpreter**, and pick `.venv`.

## 2. Run the tests and the demo (no cloud access needed)

```powershell
pytest                 # 126 tests, about one second
python -m arf demo     # builds two local sample databases with planted defects and reconciles them
```

The demo prints a report like this (abridged):

```
customers   PASS   (34 PASS, 1 SKIPPED)
inventory   ERROR  (2 ERROR, 1 SKIPPED)
   ERROR schema_fetch - target inventory: TableNotFoundError: Table 'inventory' not found ...
orders      FAIL   (22 FAIL, 3 WARN, 34 PASS, 2 SKIPPED)
   FAIL  missing_column [legacy_flag] source=STRING - Column exists in source but not in target ...
   FAIL  data_type [quantity] source=INT target=STRING - INTEGER vs STRING - type family differs
   FAIL  row_count source=1000 target=997 diff=-3 - target has 3 fewer rows
   FAIL  null_count [cust_id -> customer_id] source=0 target=4 diff=4 - NULL COUNT differs
   ...
   FAIL  missing_row [order_id=101] source=present target=absent - row exists in source but not in target
   FAIL  changed_row [order_id=600] source=status=DELIVERED target=status=CANCELLED - changed column(s): status
   ...
```

- **customers** passes, even though its timestamps differ in format and time zone (IST text vs UTC ISO-8601) and its floats differ by 1e-9. The tolerances handle both.
- **orders** catches every planted defect. The row hash names each missing and changed order. Orders 600 and 900 had their status changed, which no count or SUM can see.
- **inventory** has no target table. It is logged as ERROR with a full traceback, and the run carries on.

Open `output/demo_runs/<run_id>/arf_exceptions.csv` in Excel to see the exceptions log.

## 3. Connect to real Databricks and BigQuery

Try this first against your practice setup (Databricks Free Edition plus your GCP trial project), then the client systems.

**a. Set credentials as environment variables** (for this terminal session only; never put them in YAML or Git):

```powershell
$env:DATABRICKS_HOST      = "adb-1234567890123456.7.azuredatabricks.net"
$env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/abcdef1234567890"   # SQL warehouse > Connection details
$env:DATABRICKS_TOKEN     = "dapi..."                                 # User settings > Developer > Access tokens
$env:GCP_PROJECT          = "your-gcp-project-id"
gcloud auth application-default login                                # lets Python use your Google login
```

**b. Describe a table.** Copy `config/templates/table_template.yaml` to `config/tables/orders.yaml` and fill in the source and target table names. Add any filter, renamed columns and tolerances. Use three-part Databricks names (`catalog.schema.table`, or `hive_metastore.schema.table`) so the nullability check can run; with two-part names it is skipped.

**c. Validate, then run one table:**

```powershell
python -m arf check-config            # checks every YAML without connecting to anything
python -m arf run --only orders -v    # -v shows progress; every SQL statement is in arf.log
python -m arf run                     # all enabled tables in config/tables
```

## 4. What each check compares

| check_type | check_name | Compares | Default severity |
|---|---|---|---|
| schema | missing_column | Column exists in Databricks but not in BigQuery | critical |
| schema | extra_column | Column exists in BigQuery but not in Databricks | warning |
| schema | data_type | Type family, e.g. BIGINT = INT64, DECIMAL = NUMERIC, INT is not STRING | critical |
| schema | nullability | NOT NULL on one side only | warning |
| schema | decimal_precision | Decimal scale or integer digits reduced (e.g. DECIMAL(38,12) to NUMERIC, which keeps 9) | warning |
| row_count | row_count | `COUNT(*)` with the same filter on both sides | critical |
| numeric | sum / min / max | Numeric columns. Date and timestamp columns get MIN/MAX only | critical |
| numeric | null_count | NULLs per column (all non-nested columns by default) | critical |
| row_hash | bucket_summary, row_differences | Every row, by primary key: an MD5 hash per row compared on both sides (opt-in) | critical |
| row_hash | missing_row, extra_row, changed_row | The rows that differ, and for changed rows the columns with both values | critical |

Severity `critical` turns a mismatch into **FAIL** and the run fails. Severity `warning` turns it into **WARN**: it is logged, but the run still passes.
You can change the severity per project in `settings.yaml` or per table in its YAML.

## 4b. Row hash validation

Counts and SUM/MIN/MAX miss some defects: a status changed from SHIPPED to CANCELLED, or two values swapped between rows. Row hashing finds them and names the exact rows.

Turn it on per table:

```yaml
checks: [schema, row_count, numeric, row_hash]
primary_key: [order_id]
```

How it works, in three steps that each move very little data:

1. **Buckets.**
    - Each engine turns every row into canonical text and hashes it with MD5. Canonical text means the same value always becomes the same text on both engines, e.g. `149.50`, `2026-01-05 04:30:00.000000`, `~NULL~`.
    - It then groups the rows into 256 buckets by primary key and returns, per bucket, the row count and two sums of the hashes.
2. **Drill-down.** Only for buckets that differ, both sides return each row's key and hash. Python lists the missing, extra and changed rows.
3. **Column diff.** For up to 20 changed rows, both sides return each column's canonical text, so the result says exactly what changed, e.g. `status=SHIPPED` → `status=CANCELLED`.

Columns whose type differs between the two sides (e.g. legacy text vs a BigQuery TIMESTAMP) can't produce identical text. They are left out of the hash automatically, with a SKIPPED row saying so; the schema and numeric checks still cover them. Columns that are expected to differ, such as load times, go in `hash.exclude_columns`.

All settings are in `config/templates/table_template.yaml`.

**Cost:** a table that matches costs one scan per side. A table with differences costs up to three, the last two only reading the differing buckets' rows. On large tables, use `filter:` (e.g. one partition per run) and run row hashing less often than the cheap checks.

## 5. Tolerances

| Key | Meaning | Default |
|---|---|---|
| `row_count_abs`, `row_count_pct` | Allowed row difference, as rows or percent | exact |
| `null_count_abs`, `null_count_pct` | Same, for NULL counts | exact |
| `numeric_abs`, `numeric_rel` | SUM/MIN/MAX pass when the difference is at most max(abs, rel x value) | 1e-6, 1e-9 |
| `timestamp_seconds` | Allowed difference for timestamp MIN/MAX | 0 |
| `source_timezone`, `target_timezone` | Zone used for values that carry none (strings, TIMESTAMP_NTZ) | UTC |
| `timestamp_formats` | Extra formats for timestamps stored as strings, e.g. `%d-%m-%Y %H:%M:%S` | common ISO formats |

DATE columns are compared as calendar dates, so no time zone shift applies to them. Per-column overrides go under `column_tolerance:`.

## 6. Results and the exceptions log

Every run writes to `output/<run_id>/`:

- `arf_results.csv`: every check, for the audit trail
- `arf_exceptions.csv`: WARN, FAIL and ERROR rows only. ERROR rows carry the full traceback in `error_detail`
- `arf.log`: every SQL statement that ran, plus tracebacks

To also write results to BigQuery:

1. Run `sql/create_arf_tables.sql` once in the BigQuery console (replace `my-gcp-project`).
2. Add `bigquery` to `output.sinks` in `config/settings.yaml`.

Rows are appended with a free load job, which also works in the BigQuery sandbox.

| Exit code | Meaning |
|---|---|
| 0 | No FAIL or ERROR (WARN allowed) |
| 1 | At least one FAIL or ERROR, or results could not be written |
| 2 | Configuration error (the message names the file and key) |

## 7. Running from Airflow (later)

Copy the `arf-framework` folder into the Composer `dags/` bucket. Add the packages from `requirements.txt` to the environment's PyPI packages, and keep the Databricks token in Secret Manager (`access_token_secret:` in settings.yaml). Then:

```python
from airflow.operators.bash import BashOperator

arf_orders = BashOperator(
    task_id="arf_orders",
    bash_command="cd /home/airflow/gcs/dags/arf-framework && "
                 "python -m arf run --only orders --sinks console,bigquery",
)
load_orders >> arf_orders     # a FAIL or ERROR fails this task
```

## Project layout

```
arf-framework/
  arf/
    cli.py               command line: run, check-config, demo
    config.py            loads and validates YAML
    runner.py            runs the checks table by table
    tolerance.py         float / timestamp / date comparison rules
    hashing.py           canonical text rules for row hashing
    type_mapping.py      Databricks and BigQuery types -> common families
    sinks.py             console, CSV and BigQuery output (exception logging)
    demo.py              sample tables with planted defects
    connectors/          base.py, databricks.py, bigquery.py, sqlite.py, spark.py
    compare/             schema.py, row_count.py, numeric.py, row_hash.py
  config/
    settings.yaml        connections, output, default tolerances and severities
    tables/              one YAML per table (yours go here)
    templates/           table_template.yaml - copy this
    demo/                settings and tables used by `python -m arf demo`
  sql/create_arf_tables.sql
  tests/                 pytest suite, runs offline
```

## Version history

- **0.3.0:** row hash validation (`arf/hashing.py`, `arf/compare/row_hash.py`, hash SQL in each connector). The demo and lab plant a status change that only the hash catches.
- **0.2.0:** local Docker lab support (below).

## Local Docker lab (version 0.2.0)

The `arf-local-lab` folder next to this one runs ARF against real Spark + Delta tables and a BigQuery emulator in Docker. See its README. Version 0.2.0 added three things to ARF for it; none of them changes how ARF behaves against real Databricks and BigQuery:

- `arf/connectors/spark.py`: connection `type: spark`, a local Spark + Delta Lake session that plays Databricks. It reuses the Databricks SQL unchanged.
- `arf/connectors/bigquery.py`: optional `api_endpoint`, `anonymous_credentials` and `sum_cast` settings, for the emulator.
- `arf/demo.py`: `legacy_rows()` exposes the sample data, so the lab's notebooks load exactly the rows the demo uses.

## Known limits and next steps

- **First live run.** The Databricks and BigQuery connectors are tested with stand-in objects, not against live systems. Run them on one small table first and compare the numbers by hand once.
- **Cost.** The numeric profile scans every compared column once per run. For large tables, set `filter:` on the partition column and keep `maximum_bytes_billed`.
- **Moving legacy data.** If legacy tables are still loading during the comparison, freeze them with `as_of_version` / `as_of_timestamp` (Delta time travel).
- **String timestamps.** MIN/MAX of timestamps stored as strings is only correct for ISO-like formats (`YYYY-MM-DD ...`), because strings sort alphabetically.
- **Row hashing and floats.** FLOAT/DOUBLE values are rounded to `float_digits` before hashing. Very rarely, the two engines round a value that sits exactly on a rounding boundary differently. If a changed_row only shows a float column with a last-digit difference, lower `float_digits` or exclude that column.
- **Not built yet.** Group-by slice comparison and duplicate-key checks. Each would be a new module under `arf/compare/` and reuse the same connectors and sinks.
