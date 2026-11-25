# ARF local lab: Databricks and BigQuery on your laptop, in Docker

This lab rebuilds the Walmart migration in miniature, so you can run ARF end to end with no client access:

| At the client | In this lab |
|---|---|
| Azure Databricks notebook writing Delta tables | `notebooks/01_legacy_pipeline.ipynb` in the **legacy** container (Spark 3.5 + Delta Lake 3.3) |
| STS → GCS → Dataproc PySpark job | `notebooks/02_migration_job.ipynb`, which reads Delta, transforms it and plants defects |
| Google BigQuery | the **bigquery** container (goccy BigQuery emulator, project `local-project`) |
| `python -m arf run` from Composer | `notebooks/03_run_arf.ipynb`, the same command with the lab settings |

## Folder layout

Keep both folders side by side. The lab mounts `../arf-framework` into its container.

```
arf-workspace/
  arf-framework/        ARF itself (unchanged by the lab; the spark connector and emulator options live here)
  arf-local-lab/        this folder
    docker-compose.yml        two containers: legacy, bigquery
    docker/legacy/            Dockerfile + requirements for the legacy container
    notebooks/                01, 02, 03 + lab_common.py (shared helpers built on ARF's code)
    arf-config/               ARF settings.yaml + table YAMLs for the lab
    data/                     Delta files and the emulator database (created when you run)
    output/                   ARF results, one folder per run
```

## What you need

- **Docker Desktop**, running. Give it at least 4 GB of memory under Settings > Resources.
- **Disk:** about 3 GB free for the images.
- **Internet for the first build:**
  - the Python base image from Docker Hub
  - Debian packages (Java)
  - PyPI (PySpark, Delta, Jupyter)
  - Maven Central (Delta jars)
  - ghcr.io (the emulator)

  If your office network blocks any of these, set the company proxy in Docker Desktop under Settings > Resources > Proxies.

## Run it

```powershell
cd arf-workspace\arf-local-lab
docker compose up -d --build        # first build takes several minutes
docker compose ps                   # both containers should be "running"
```

1. Open **http://localhost:8888/lab?token=arf**.
2. In JupyterLab, open `notebooks/` and run, in order, **01**, **02**, **03** (Run > Run All Cells for each).
3. Notebook 03 shows the result, and the CSV files appear in `arf-local-lab/output/<run_id>/`.

**Expected result** (the same as `python -m arf demo`):

| Table | Status | What ARF found |
|---|---|---|
| customers | PASS | Nothing wrong: the IST-to-UTC change and the float noise are within tolerance |
| orders | FAIL | 3 rows missing, 4 NULL customer ids, 2 amounts changed, `legacy_flag` dropped, `quantity` INT → STRING. Warnings: NOT NULL lost, `unit_price` scale 12 → 9, extra `load_ts`. The row hash names every missing and changed order, including the 2 whose status changed, which no count or SUM can see |
| inventory | ERROR | The target table was never migrated; ARF logs it and carries on |

You can also run ARF from a terminal instead of notebook 03:

```powershell
docker compose exec legacy bash -c "cd /arf && python -m arf run --settings /lab/arf-config/settings.yaml --tables /lab/arf-config/tables"
```

## Stop, restart, reset

- **Stop:** `docker compose down`. Your Delta tables and emulator data stay in `data/`.
- **Start again:** `docker compose up -d`. This doesn't rebuild the image, so it's fast.
- **Full reset:** run `docker compose down`, delete everything inside `data/` and `output/` except the `.gitkeep` files, then start again and rerun notebooks 01 and 02.

## How the lab uses ARF

The lab does not copy any ARF code; it imports it.

- `notebooks/lab_common.py` builds Spark with `arf.connectors.spark.build_spark_session`. It builds the BigQuery client with ARF's `BigQueryConnector`, reading both from `arf-config/settings.yaml`.
- Notebook 01 takes its rows from `arf.demo.legacy_rows()`. Notebook 02 takes its planted defects from `arf.demo` (`MISSING_ORDER_IDS` and the others).
- `arf-config/settings.yaml` uses the same connection names as `arf-framework/config/settings.yaml`. Only the types differ: `spark` instead of `databricks`, and an `api_endpoint` for the emulator.

The ARF guide ("How It Works, File by File") has a diagram of these links.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Build stops while downloading | Office proxy: set it in Docker Desktop > Settings > Resources > Proxies, then `docker compose build --no-cache` |
| Port 8888 or 9050 already in use | In `docker-compose.yml`, change the left number, e.g. `"8899:8888"`, and open that port instead |
| Notebook 01's first cell can't reach the emulator | `docker compose ps`; if `arf-bigquery` isn't running, check `docker compose logs bigquery` |
| `Table ... not found in local Spark` | Run notebook 01 first. Its Delta folders must exist under `data/delta/legacy_sales/` |
| ARF numeric check ERROR mentioning BIGNUMERIC | In `arf-config/settings.yaml`, uncomment `sum_cast: NUMERIC` under `bigquery_target` |
| `row_hash` rows show ERROR on the BigQuery side only | The emulator may not support a function the hash uses (MD5, FORMAT, FORMAT_TIMESTAMP, CODE_POINTS_TO_STRING). Real BigQuery supports all of them. Remove `row_hash` from `checks:` in `arf-config/tables/*.yaml` to run the other checks, and keep the error text so it can be fixed |
| Java out of memory, or a kernel dies | Shut down the kernels of the other notebooks, or raise Docker Desktop's memory limit |
| `ModuleNotFoundError: arf` | Make sure `arf-framework` sits next to `arf-local-lab`, then run `docker compose up -d` again |

## Status

This lab was written and tested without Docker: ARF's Spark connector ran end to end against a stand-in Spark session, and every notebook cell was checked for syntax. **Your first `docker compose up` is the first time these containers run.** If a step fails, keep the error text and the notebook cell; that's all that's needed to fix it.
