"""Command line entry point.

    python -m arf demo
    python -m arf check-config --settings config/settings.yaml --tables config/tables
    python -m arf run          --settings config/settings.yaml --tables config/tables [--only orders,customers]

Exit codes: 0 = all checks passed (WARN allowed), 1 = any FAIL or ERROR, 2 = configuration error.
Airflow treats a non-zero exit code as a failed task.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import ConfigError, Settings, TableConfig, load_settings, load_tables
from .connectors import create_connector
from .runner import RunSummary, Runner, new_run_id
from .sinks import BigQuerySink, ConsoleSink, CsvSink, Sink

log = logging.getLogger("arf")


def setup_logging(run_dir: Path | None, verbose: bool, console_sink: bool = False) -> None:
    log.setLevel(logging.DEBUG)
    log.propagate = False
    teardown_logging()
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.INFO if verbose else logging.WARNING)
    console.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    if console_sink:   # the console sink already prints check errors; don't print them twice
        console.addFilter(lambda record: not getattr(record, "result_row", False))
    log.addHandler(console)
    if run_dir:
        run_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(run_dir / "arf.log", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)   # every SQL statement and full tracebacks
        file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s"))
        log.addHandler(file_handler)


def teardown_logging() -> None:
    for handler in list(log.handlers):
        handler.close()
        log.removeHandler(handler)


def build_sinks(settings: Settings, names: list[str], run_dir: Path) -> list[Sink]:
    sinks: list[Sink] = []
    for name in names:
        if name == "console":
            sinks.append(ConsoleSink())
        elif name == "csv":
            sinks.append(CsvSink(run_dir))
        elif name == "bigquery":
            out = settings.output
            if not (out.bigquery_connection and out.results_table and out.exceptions_table):
                raise ConfigError("sink 'bigquery' needs output.bigquery.connection, results_table and "
                                  "exceptions_table in settings.yaml")
            conn = create_connector(out.bigquery_connection, settings.connections[out.bigquery_connection])
            sinks.append(BigQuerySink(conn, out.results_table, out.exceptions_table))
        else:
            raise ConfigError(f"Unknown sink '{name}'")
    return sinks


def execute(settings: Settings, tables: list[TableConfig], sink_names: list[str] | None = None,
            run_id: str | None = None, verbose: bool = False) -> RunSummary:
    """Run ARF for the given tables and return the summary. Used by the CLI, the demo and tests."""
    run_id = run_id or new_run_id()
    run_dir = Path(settings.output.output_dir) / run_id
    sink_names = sink_names or settings.output.sinks
    setup_logging(run_dir, verbose, console_sink="console" in sink_names)
    try:
        sinks = build_sinks(settings, sink_names, run_dir)
        log.info("ARF run %s: %d table(s)", run_id, len(tables))
        return Runner(settings, sinks, run_id=run_id, run_dir=run_dir).run(tables)
    finally:
        teardown_logging()


def _cmd_run(args) -> int:
    settings = load_settings(args.settings)
    only = [t.strip() for t in args.only.split(",")] if args.only else None
    tables = load_tables(args.tables, settings, only)
    if not tables:
        print("No enabled tables found - nothing to do.")
        return 0
    sinks = [s.strip() for s in args.sinks.split(",")] if args.sinks else None
    summary = execute(settings, tables, sinks, args.run_id, args.verbose)
    for err in summary.sink_errors:
        print(f"ERROR {err}", file=sys.stderr)
    return summary.exit_code


def _cmd_check_config(args) -> int:
    settings = load_settings(args.settings)
    tables = load_tables(args.tables, settings)
    print(f"Settings OK: {len(settings.connections)} connection(s): {', '.join(settings.connections)}")
    print(f"{len(tables)} enabled table(s):")
    for t in tables:
        print(f"  {t.table_id:<28} {t.source.table} ({t.source.connection}) -> "
              f"{t.target.table} ({t.target.connection})  checks={','.join(t.checks)}")
    return 0


def _cmd_demo(args) -> int:
    from .demo import run_demo
    return run_demo(verbose=args.verbose)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m arf", description="Automated Reconciliation Framework")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="reconcile tables")
    p_run.add_argument("--settings", default="config/settings.yaml")
    p_run.add_argument("--tables", default="config/tables", help="a table YAML file or a folder of them")
    p_run.add_argument("--only", help="comma-separated table_ids to run (default: all enabled)")
    p_run.add_argument("--sinks", help="override output sinks, e.g. console,csv")
    p_run.add_argument("--run-id", help="custom run id (default: timestamp + random suffix)")
    p_run.add_argument("-v", "--verbose", action="store_true", help="show progress messages")
    p_run.set_defaults(func=_cmd_run)

    p_chk = sub.add_parser("check-config", help="validate YAML files without connecting to anything")
    p_chk.add_argument("--settings", default="config/settings.yaml")
    p_chk.add_argument("--tables", default="config/tables")
    p_chk.set_defaults(func=_cmd_check_config)

    p_demo = sub.add_parser("demo", help="run ARF on local sample tables with planted defects")
    p_demo.add_argument("-v", "--verbose", action="store_true")
    p_demo.set_defaults(func=_cmd_demo)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    except ImportError as exc:   # e.g. BigQuery sink without google-cloud-bigquery installed
        print(f"Missing package: {exc}", file=sys.stderr)
        return 1
