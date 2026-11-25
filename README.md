# ARF Framework: How It Works, File by File

Nov 24, 2026 · @Abhigyan

ARF (WBS 1.4) is a working template. It runs today on local sample data. Once the client environments exist, it connects to Databricks and BigQuery through YAML settings and environment variables, with no code changes. This guide explains every file, how the files connect, and why each one is built the way it is.

## How a run flows

`cli.py` starts every run, `runner.py` runs the checks, and only the files in `connectors/` ever talk to an engine.

&#91;embedded content: ARF files · who calls whom\]

Read the diagram from top to bottom:

1. `cli.py` loads the config, builds the sinks and starts the runner.
2. The runner asks `connectors/` for numbers and asks `compare/` to judge them.
3. Results go to `sinks.py`.

`models.py` sits under everything: it defines what a column and a result row look like.

## Which file touches which environment

Only four files reach outside your machine. ARF only reads the legacy and migrated tables; its only writes go to its own `arf` dataset and to local files.

&#91;embedded content: ARF files · which system each one reaches\]

This is the access list to request at kickoff. Which connection each file uses is set by the `connection:` names in `config/settings.yaml`.

| Environment | Reached by | What ARF does there | Access it needs |
| --- | --- | --- | --- |
| Azure Databricks SQL warehouse | `connectors/databricks.py` | Reads column metadata; runs `COUNT(*)` and one aggregate `SELECT` per table; never writes | `CAN USE` on the warehouse; `USE CATALOG`, `USE SCHEMA` and `SELECT` on legacy tables; a token |
| BigQuery: migrated tables | `connectors/bigquery.py` | Reads table metadata; runs `COUNT(*)` and one aggregate query per table; never writes | BigQuery Data Viewer on the target datasets; BigQuery Job User on the query project |
| BigQuery: `arf` dataset | `sinks.py` (BigQuerySink), through `bigquery.py` | Appends rows to `arf_results` and `arf_exceptions` | BigQuery Data Editor on the `arf` dataset |
| Secret Manager | `connectors/base.py` (`resolve_secret`) | Reads the Databricks token when `access_token_secret` is set | Secret Manager Secret Accessor |
| Local disk (laptop or Composer) | `sinks.py` (CsvSink), `cli.py`, `connectors/sqlite.py` | Writes `output/<run_id>/` CSVs and `arf.log`; the demo reads and writes `.db` files | None |

## One run, step by step

A run follows the same seven steps for every table. Here they are for `python -m arf run --only orders`, with the function that does each step.

1. **Start.** `arf/__main__.py` hands the command to `cli.main()`, which routes it to `_cmd_run()`.
2. **Read config.** `config.load_settings()` reads `config/settings.yaml`, and `config.load_tables()` reads the table YAML files. A typo, an unknown connection or a missing key stops the run here with exit code 2.
3. **Prepare the run.** `cli.execute()` creates a run id such as `20261124T073152Z_94469f`, the folder `output/<run_id>/` and the log file `arf.log`. It then builds the sinks named in `output.sinks`.
4. **Check each table.** `Runner.run()` loops over the tables and calls `run_table()` for each one:
   - `ConnectorPool.get()` opens the Databricks and BigQuery connections once and reuses them for every table.
   - `get_schema()` reads the column list on each side from metadata, without scanning the table.
   - `compare/schema.py` pairs the columns (applying `column_mapping` and `ignore_columns`), then checks types, nullability and decimals.
   - `compare/row_count.py` runs `COUNT(*)` on each side, each with its own filter.
   - `compare/numeric.py` works out which aggregates each column needs. `base.profile()` then runs one `SELECT` per side, and `tolerance.py` compares every value.
5. **Record results.** `compare.ResultBuilder` turns each comparison into one `CheckResult` row. Any exception becomes an ERROR row that carries its traceback, and the next check and the next table still run.
6. **Write results.** After each table, every sink gets that table's rows:
   - `ConsoleSink` prints them.
   - `CsvSink` appends to `arf_results.csv` and `arf_exceptions.csv`.
   - `BigQuerySink` holds them until the end of the run.
7. **Finish.** Connections close, and `BigQuerySink` loads its rows into BigQuery. The console prints the summary and the exit code goes back to whoever started the run: 0 = passed, 1 = a FAIL or ERROR, 2 = a config error.

## Core files in arf/

These ten files hold the logic. None of them talks to Databricks or BigQuery directly; that is left to `connectors/`.

### `__init__.py`

- Marks `arf/` as a Python package and holds the version number (0.1.0).

### `__main__.py`

- Makes `python -m arf ...` work. It only calls `cli.main()` and exits with its return code.
- **Why:** you and Airflow start ARF the same way, and Airflow receives the exit code.

### `cli.py`: the command line

- **Commands:** `run` reconciles tables, `check-config` validates the YAML without connecting, and `demo` runs the sample.
- **Options:** `--only orders,customers`, `--sinks console,csv`, `--run-id`, and `-v` to show progress.
- **One entry point:** `execute()` is the single function that starts a run. The CLI, the demo and the tests all call it, so they behave the same way.
- **Logging:** sets up two outputs. The console shows warnings, and `arf.log` records every SQL statement and full tracebacks.
- **Why:** exit codes 0, 1 and 2 let Airflow fail a task automatically, and `check-config` catches YAML mistakes before anyone touches a database.

### `config.py`: reads and validates YAML

- **Loading:** `load_settings()` reads connections, outputs, and the default tolerances and severities. `load_tables()` reads one file or a whole folder, and a file can hold one table or a `tables:` list.
- **Validation:** rejects unknown keys (a typo such as `fliter`), connections that are not defined, invalid check names and non-numeric tolerances. The message names the file and the key.
- **Environment variables:** expands values like `${GCP_PROJECT}`, so the same files work in dev and prod with no secrets in Git.
- **Merging:** builds each table's settings as defaults, then the table's `tolerance:`, then `column_tolerance:`. The most specific setting wins.
- **Why:** with about 300 tables, adding a table has to mean adding a YAML file, not writing code. Strict validation prevents a quiet comparison of the wrong data.

### `models.py`: shared data shapes

- `ColumnInfo` describes one column: its name, raw type, type family, nullability, precision and scale.
- `CheckResult` is one result row with 16 fields. `Status` is one of PASS, WARN, FAIL, ERROR or SKIPPED.
- `EXCEPTION_STATUSES` decides which rows go to the exceptions log. `BLOCKING_STATUSES` decides which rows fail the run.
- **Why:** a result row is defined in one place, so the console, the CSVs and BigQuery always hold the same columns.

### `type_mapping.py`: one vocabulary for two engines

- **Families:** maps Databricks type names (BIGINT, DOUBLE, DECIMAL(18,2), TIMESTAMP\_NTZ) and BigQuery type names (INT64, FLOAT64, NUMERIC, DATETIME) to shared families such as INTEGER, FLOAT, DECIMAL and DATETIME.
- **Decimal defaults:** fills in precision and scale when an engine leaves them out. BigQuery NUMERIC is (38,9), BIGNUMERIC is (76,38), and Databricks DECIMAL is (10,0).
- **Compatibility:** `is_compatible()` accepts types in the same family, plus any pairs listed in `allowed_type_changes`.
- **Why:** BIGINT vs INT64 is not a defect, but INT vs STRING is.

### `tolerance.py`: when two values count as equal

- `compare_counts()` handles row counts and NULL counts. They must match exactly unless you allow a number of rows or a percentage.
- `compare_numeric()` handles SUM, MIN and MAX. A value passes when the gap is no more than the larger of the absolute tolerance and relative tolerance × value. It handles Decimal, float, NaN and NULL.
- `compare_temporal()` converts both timestamps to UTC first. Values with no time zone are read in the zone you configure, and string timestamps are parsed from their format.
- `compare_dates()` compares calendar dates, with no time zone shift.
- **Why:** float sums change slightly with row order, and timestamps differ in format and time zone between engines. Without these rules nearly every table would fail when nothing is actually wrong.

### `runner.py`: runs the checks

- **Order:** for each table, `Runner.run_table()` connects, reads both schemas, then runs the schema check, the row count and the numeric profile.
- **Errors:** every step has its own try/except. A failure becomes an ERROR row and the run moves on.
- **Summary:** `RunSummary` counts statuses per table and sets the exit code.
- **Why:** one broken table out of 300 must not hide the results of the other 299.

### `sinks.py`: exception logging

- **Three sinks:**
  - `ConsoleSink` prints a summary for each table, plus its problem rows.
  - `CsvSink` writes `arf_results.csv` (every check) and `arf_exceptions.csv` (WARN, FAIL and ERROR only).
  - `BigQuerySink` appends the same rows to two BigQuery tables.
- **Excel:** the CSV files are saved as UTF-8 with a BOM, so Excel opens them correctly.
- **Upload failures:** if the BigQuery upload fails, the run is marked as failed rather than silently losing results.
- **Why:** this is the "automated exception logging" in the WBS. The exceptions table is the to-do list, and the results table is the audit trail.

### `demo.py`: sample data with planted defects

- **Sample data:** builds `legacy_databricks.db` and `target_bigquery.db` from seeded random data, so every run produces the same values. The three tables are:
  - customers: only harmless differences.
  - orders: 8 planted defects.
  - inventory: the target table is missing.
- **Run:** reconciles the two files using the settings in `config/demo/`.
- **Why:** it shows the framework working before any environment exists, and the end-to-end tests reuse it.

## connectors/ and compare/

`connectors/` is the only code that talks to an engine, and `compare/` is the only code that decides PASS or FAIL. Keeping the two apart is what lets the demo run on SQLite and the real run use Databricks and BigQuery, with no change to the comparison logic.

### `connectors/__init__.py`: picks and reuses connections

- `create_connector()` picks a class from the connection's `type:` setting: `databricks`, `bigquery` or `sqlite`.
- `ConnectorPool` opens each connection once per run. It also remembers failures, so a wrong token is reported once instead of being retried 300 times.

### `connectors/base.py`: the contract every engine follows

- **The contract:** every engine implements `get_schema()`, `_execute()`, `quote()` and `table_ref()`.
- **Shared SQL:** `row_count()` writes `SELECT COUNT(*) ... WHERE (filter)`. `profile()` writes a single `SELECT` that holds every SUM, MIN, MAX and NULL count for the table, aliased `m0`, `m1`, and so on. Very wide tables are split into chunks of 300.
- **Secrets:** `resolve_secret()` looks for a token in this order:
  1. an environment variable
  2. Secret Manager
  3. plain text in YAML, which logs a warning
- **Why:** the comparison code never knows which engine it is talking to, so adding an engine means writing one new class.

### `connectors/databricks.py`: the legacy side

- **Connection:** uses `databricks-sql-connector` to reach a SQL warehouse with `server_hostname`, `http_path` and a token, then runs `SET TIME ZONE 'UTC'`.
- **Schema:** for three-part names it calls `cursor.columns()`, which includes nullability. For two-part names it falls back to `DESCRIBE TABLE`.
- **SQL details:**
  - Names are quoted with backticks.
  - `VERSION AS OF` and `TIMESTAMP AS OF` give Delta time travel.
  - Integer sums are cast to DECIMAL(38,0), so BIGINT totals cannot overflow.
- **Environment:** Azure Databricks.

### `connectors/bigquery.py`: the target side

- **Login:** uses `google-cloud-bigquery` with Application Default Credentials. That means your `gcloud` login on a laptop, and the service account on Composer.
- **Schema:** comes from `client.get_table()`, which reads metadata and has no query cost. REPEATED columns are reported as ARRAY, and REQUIRED columns as NOT NULL.
- **Queries:**
  - Integer and decimal sums are cast to BIGNUMERIC, so they cannot overflow.
  - Every job carries the label `tool=arf`, so ARF's query costs can be tracked.
  - `maximum_bytes_billed` caps how much data one query may scan.
  - `FOR SYSTEM_TIME AS OF` gives time travel.
- **Writing results:** `load_rows()` appends result rows with a load job. Load jobs are free and work in the BigQuery sandbox.
- **Environment:** the migrated BigQuery tables and the `arf` results dataset.

### `connectors/sqlite.py`: local testing

- Reads local SQLite files. The `emulate:` setting says whether the column types use Databricks names or BigQuery names.
- **Why:** the whole pipeline can run and be tested on a laptop with no cloud access.

### `compare/__init__.py`: ResultBuilder

- Fills the 16 result fields the same way for every check, and turns "passed or not" plus the severity into PASS, WARN or FAIL.
- `error()` writes an ERROR row with its traceback and logs it.

### `compare/schema.py`

- **Matching columns:** `align_columns()` pairs source and target columns by name, ignoring case, and applies `column_mapping` and `ignore_columns`. Source columns with no partner are missing; target columns with no partner are extra.
- **Checks for each pair:** type family, nullability, and decimal precision (flagged when the scale or the number of integer digits gets smaller).
- `numeric.py` reuses `align_columns()`, so both checks work on exactly the same column pairs.

### `compare/row_count.py`

- Runs `row_count()` on both sides, each with its own filter, and compares the counts using `row_count_abs` and `row_count_pct`.

### `compare/numeric.py`

- **Planning:** `plan_metrics()` decides what to compute for each column:
  - numeric: SUM, MIN and MAX
  - timestamp: MIN and MAX, compared in UTC
  - date: MIN and MAX, compared as calendar dates
  - string: NULL count only
  - nested types: skipped
  - types that differ between the two sides: SKIPPED, with a pointer to the schema check
- **Running:** `run_numeric()` sends one profile query to each side, then compares every value using that column's tolerance.
- **Why:** one scan per side keeps cost and run time low, even on large tables.

## Config, SQL, tests and root files

The YAML files describe what to compare and are the only files you edit for a new environment or a new table.

### `config/settings.yaml`: the real environments

- **Connections:** defines two, `databricks_legacy` and `bigquery_target`. Their values come from the environment variables `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` and `GCP_PROJECT`.
- **Other settings:** also holds the output sinks, the names of the BigQuery results tables, and the default tolerances and severities.
- **Moving between environments:** going from laptop to client dev to prod only changes environment variables, not this file.

### `config/tables/`

- Holds your table list: one YAML file per table, or many tables in one file. It stays empty until the requirements arrive; `python -m arf run` reads everything in it.

### `config/templates/table_template.yaml`

- A commented example of every option a table can have: filter, Delta time travel, renamed columns, ignored columns, column lists, tolerances, and severity overrides. Copy it to start a new table.

### `config/demo/`

- The settings file and three table files used by the demo. The connections point to the two SQLite files through `${ARF_DEMO_DIR}`, which `demo.py` sets.

### `sql/create_arf_tables.sql`

- Creates the BigQuery tables `arf_results` and `arf_exceptions`, partitioned by run date and clustered by table and status. Run it once.
- Also includes a query that lists the problems from the latest run.

### `tests/`: 126 tests, all offline

| Test file | What it proves |
| --- | --- |
| `conftest.py` | Shared setup: temporary settings with SQLite connections named like the real ones |
| `test_type_mapping.py` | Databricks and BigQuery types land in the right families; decimal defaults are correct |
| `test_tolerance.py` | Float noise passes and real gaps fail; IST vs UTC and format differences pass; dates are not shifted |
| `test_config.py` | Typos, unknown connections and duplicates are rejected; overrides merge; `${VAR}` expands |
| `test_schema_compare.py` | Missing, extra and renamed columns; type mismatches; nullability; decimals; severity overrides |
| `test_numeric_plan.py` | Each column gets the right metrics; mismatched types are skipped |
| `test_connectors_sql.py` | The exact SQL generated for Databricks and BigQuery; metadata is parsed correctly when fake objects stand in for the real libraries |
| `test_end_to_end.py` | WBS acceptance: every planted defect is found and logged; exit codes; a broken connection does not crash the run |

### Root files

- `README.md`: setup, demo and run steps for Windows and VS Code.
- `requirements.txt`: lists the Python packages:
  - `pyyaml` reads the YAML files.
  - `tzdata` provides time zone names on Windows.
  - `databricks-sql-connector` and `google-cloud-bigquery` connect to the two engines.
  - `google-cloud-secret-manager` is needed only if tokens are kept in Secret Manager.
  - `pytest` runs the tests.
- `pytest.ini`: tells pytest where the tests live.
- `.gitignore`: keeps `.venv`, `output/`, `.db` files and key files out of Git.

## Design decisions and the reasoning

Each decision below solves a specific risk in a 300-table migration. Use this table when your manager or the client asks "why did you build it this way?"

| Decision | Reasoning | Where |
| --- | --- | --- |
| Each engine computes its own aggregates; only one row per side comes back | No client data is copied between Azure and GCP. It stays fast on large tables and costs one scan per side. | `base.py`, `numeric.py` |
| Driven by config: one YAML per table | Adding table 301 means adding a file, not code. Reviewers can read what is compared. | `config.py` |
| One connector interface | Comparison logic is the same for every engine. SQLite stands in for testing, and new engines are easy to add. | `connectors/base.py` |
| Type families instead of exact type names | The two engines name the same types differently (BIGINT vs INT64), and that is not a defect. | `type_mapping.py` |
| Absolute + relative tolerance; timestamps converted to UTC; dates left as dates | Float sums vary with row order, and timestamps differ in format and time zone, so false failures would bury real ones. | `tolerance.py` |
| Severity: critical vs warning | Not every difference should block a release. An extra audit column is worth noting but not worth stopping for. | `settings.yaml`, `ResultBuilder` |
| Separate results and exceptions outputs | Results are the audit trail of every check; exceptions are the list someone has to act on. | `sinks.py` |
| An error becomes an ERROR row instead of a crash | One bad table or a timeout must not hide the results of every other table. | `runner.py` |
| Exit codes 0, 1 and 2 | Airflow fails the task automatically, so a bad load stops the pipeline. | `cli.py` |
| Secrets come from environment variables or Secret Manager | No token or password ever sits in YAML or Git. | `resolve_secret()` |
| Results reach BigQuery through load jobs, not streaming inserts | Load jobs are free and work in the BigQuery sandbox; streaming inserts cost money and do not work there. | `bigquery.py` |
| Sums cast to DECIMAL(38,0) or BIGNUMERIC | BIGINT and INT64 totals can overflow on large tables. | `databricks.py`, `bigquery.py` |
| Unknown YAML keys are rejected | Otherwise a typo such as `fliter` is silently ignored and the wrong rows get compared. | `config.py` |
| Delta time travel options | If legacy tables keep loading during the comparison, freezing a version keeps the numbers stable. | `databricks.py`, table YAML |

## Will it work at the client?

Yes, as a template. Nothing in the code depends on client details: every table, connection and threshold lives in YAML and environment variables. The part not yet proven is the live connection to the client systems. It is built on the documented Databricks and BigQuery libraries and tested with stand-in objects, but it has never run against a real workspace.

**What works today, with no environment:**

- `python -m arf demo` runs end to end and catches all of its planted defects.
- `pytest` runs 126 tests offline, including the exact SQL sent to each engine.
- `python -m arf check-config` validates the YAML.

**What is assumed, and what changes if an assumption is wrong:**

| Assumption | If it turns out different | What changes |
| --- | --- | --- |
| Databricks is reached through a SQL warehouse with a personal access token | Client uses a service principal or Azure AD (OAuth) login | The `connect()` call in `databricks.py`, a few lines |
| The machine running ARF can reach Azure Databricks over the network | Firewall blocks it from Composer | Run ARF from a VM that can reach both; no code change |
| Unity Catalog names (`catalog.schema.table`) | Tables are in the older `hive_metastore` | Nothing: write `hive_metastore.schema.table` in the YAML |
| BigQuery login uses `gcloud` on a laptop and a service account on Composer | Policy requires key files | Nothing: set `GOOGLE_APPLICATION_CREDENTIALS` |
| Composer runs Python 3.10+ and allows these packages | Older Python, or locked-down packages | Pin versions in `requirements.txt` |
| Legacy tables stay still while ARF compares them | Legacy pipelines keep loading during migration | Add `as_of_version` or a date `filter` in the table YAML |
| The default tolerances and severities are acceptable | Client wants different thresholds | `settings.yaml` only |

**Ask for these at kickoff:**

- [ ] Databricks workspace URL, SQL warehouse HTTP path, and the login method (token or service principal)
- [ ] `CAN USE` on the SQL warehouse, plus `USE CATALOG`, `USE SCHEMA` and `SELECT` on the legacy tables
- [ ] A GCP project for ARF queries: BigQuery Job User on the project, BigQuery Data Viewer on the target datasets
- [ ] A dataset for `arf_results` and `arf_exceptions`, with BigQuery Data Editor
- [ ] Can Composer (or another runner) reach Azure Databricks over the network?
- [ ] Composer's Python version and its policy on installing packages
- [ ] Will legacy tables keep loading during the migration?
- [ ] Acceptance rules: exact match or tolerance, and which checks block a release
- [ ] A pilot list of tables, with their filters and any renamed columns

**First live run:** pick one small table, run `python -m arf run --only <table> -v`, and check its row count and one SUM by hand in both consoles. Once those match, the connectors are proven and the remaining tables only need YAML.

## Row hash validation (ARF 0.3.0)

Row hashing compares every row by primary key. ARF can now find rows that are missing, extra or changed even when every count, SUM, MIN and MAX matches, such as a status changed from SHIPPED to CANCELLED. It is opt-in per table, because it reads every column.

**How it works.** Each of the three steps moves very little data between the clouds:

1. **Buckets.** Each engine turns every row into canonical text: the same value always becomes the same text on both engines. It hashes that text with MD5 and groups the rows into 256 buckets by primary key. Per bucket, it returns the row count and two sums of the hashes. Only 256 small rows per side come back, and if they match, the table passes.
2. **Drill-down.** Only for buckets that differ, both sides return each row's key and hash. Python then lists the missing, extra and changed rows.
3. **Column diff.** For up to 20 changed rows, both sides return each column's canonical text, so the result names the changed columns and shows both values.

**Same value, same text, on both engines.** This is what makes the hashes comparable:

| Type | Canonical text | Databricks / Spark SQL | BigQuery SQL |
| --- | --- | --- | --- |
| NULL | `~NULL~` | `CASE WHEN col IS NULL ...` | same |
| STRING | as stored | `col` | `col` |
| INTEGER, BOOLEAN | `1001`, `true` | `CAST(col AS STRING)` | `CAST(col AS STRING)` |
| DECIMAL | `149.50`, using the smaller scale of the two sides | `format_string('%.2f', col)` | `FORMAT('%.2f', col)` |
| FLOAT | `0.123457` (6 decimals) | `format_string('%.6f', col)` | `FORMAT('%.6f', col)` |
| DATE | `2026-01-05` | `date_format(col, 'yyyy-MM-dd')` | `FORMAT_DATE('%Y-%m-%d', col)` |
| TIMESTAMP | `2026-01-05 04:30:00.000000` (UTC) | `date_format(col, 'yyyy-MM-dd HH:mm:ss.SSSSSS')` | `FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', col, 'UTC')` |

**Joining and hashing.** Columns are joined with the ASCII unit separator (code 31) and hashed with `md5()` in Spark or `TO_HEX(MD5())` in BigQuery. I checked the BigQuery functions against Google's documentation:

- `FORMAT('%f')` accepts NUMERIC, BIGNUMERIC and FLOAT64 ([string functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/string_functions)).
- `%E6S` gives six fractional digits of seconds ([format elements](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/format-elements)).
- Hex strings such as `0x1a` cast to INT64 ([conversion functions](https://docs.cloud.google.cn/bigquery/docs/reference/standard-sql/conversion_functions)).

**Columns that are left out.** A column whose type differs between the two sides can never produce identical text, e.g. legacy IST text vs a BigQuery TIMESTAMP. It is left out of the hash automatically, with a SKIPPED row saying why; the schema and numeric checks still cover it. Columns expected to differ, such as load timestamps, go in `hash.exclude_columns`.

**What the exceptions log shows** (from the demo):

| check\_name | Example |
| --- | --- |
| `bucket_summary` | 1000 rows vs 997 rows; 11 of 256 buckets differ |
| `row_differences` | 3 missing, 0 extra, 8 changed |
| `missing_row` | `order_id=101`: present in source, absent in target |
| `changed_row` | `order_id=600`: `status=DELIVERED` → `status=CANCELLED` |

**Turning it on** for a table:

```yaml
checks: [schema, row_count, numeric, row_hash]
primary_key: [order_id]          # source column(s) that identify a row
hash:
  exclude_columns: [etl_load_ts]   # optional
```

**Files involved:**

- `arf/hashing.py` holds the canonical text rules.
- `arf/compare/row_hash.py` runs the three steps.
- The hash SQL pieces live in each connector: `base.py`, `databricks.py` (inherited by `spark.py`), `bigquery.py` and `sqlite.py`.
- The tests prove that Spark-style hash SQL and the SQLite side produce byte-identical hashes for every unchanged row, and that a swapped status is caught when every other check passes.

**Cost and limits:**

- A table that matches costs one scan per side. A table with differences costs up to three, and the last two only return rows from the differing buckets.
- On large tables, filter to one partition per run and schedule row hashing less often than the cheap checks.
- FLOAT values are rounded before hashing. Very rarely the two engines round a boundary value differently; lower `float_digits` or exclude that column.
- The BigQuery emulator in the lab may not support every hash function; real BigQuery does.

## Local Docker lab and how it connects to ARF

The lab (`arf-local-lab/`) runs ARF end to end on your laptop: a Spark + Delta container plays Databricks, and a BigQuery emulator plays BigQuery. It imports ARF from `../arf-framework` rather than copying it, so every lab run exercises the same code you will run at the client.

&#91;embedded content: arf-local-lab files · what each one uses in arf-framework\]

The two folders sit side by side, and `docker-compose.yml` mounts `arf-framework` into the legacy container at `/arf`. Three ARF files changed for the lab (version 0.2.0). Each change is additive: a new connection type, optional settings, and a function that exposes the demo data.

| Lab file | What it does | What it uses from ARF | Runs in |
| --- | --- | --- | --- |
| `docker-compose.yml` | Starts the two containers. Mounts the lab at `/lab` and ARF at `/arf` | The whole `arf-framework/` folder | Your laptop (Docker) |
| `docker/legacy/Dockerfile` | Builds Python 3.11, Java 17, Spark 3.5, Delta 3.3 and JupyterLab; downloads the Delta jars once | Nothing (installs the packages ARF needs) | Build time |
| `notebooks/lab_common.py` | Shared helpers: `get_spark()`, `bq_client()`, table paths | `arf.config.load_settings`, `arf.connectors.spark.build_spark_session`, `BigQueryConnector` | legacy container |
| `notebooks/01_legacy_pipeline.ipynb` | Creates `legacy_sales.customers`, `orders`, `inventory` as Delta tables | `arf.demo.legacy_rows()`, `lab_common` | legacy container |
| `notebooks/02_migration_job.ipynb` | Reads Delta, transforms it, plants defects, writes to `local-project.sales` | `arf.demo` defect lists, `lab_common` | legacy container → bigquery container |
| `notebooks/03_run_arf.ipynb` | Runs `python -m arf run` and shows the exceptions | `arf/cli.py`, and through it the whole ARF run | legacy container |
| `arf-config/settings.yaml` | Lab connections: `type: spark` and the emulator's `api_endpoint` | Read by `arf/config.py`; same connection names as `config/settings.yaml` | Read by ARF |
| `arf-config/tables/*.yaml` | customers, orders, inventory; same shape as client table files | Read by `arf/config.py` | Read by ARF |

**What changed in ARF 0.2.0:**

- `arf/connectors/spark.py` (new): `build_spark_session()` and `SparkConnector`. It inherits the Databricks SQL unchanged, because both engines run Spark SQL on Delta.
- `arf/connectors/bigquery.py`: optional `api_endpoint`, `anonymous_credentials` and `sum_cast` settings. Real BigQuery runs ignore them.
- `arf/demo.py`: `legacy_rows()` exposes the sample rows; the demo's own data is unchanged, byte for byte.
- Registration of the `spark` type in `connectors/__init__.py` and `config.py`, plus 6 new tests.
