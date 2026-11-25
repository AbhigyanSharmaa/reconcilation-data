"""Automated Reconciliation Framework (ARF).

Compares a legacy Databricks Delta table with its migrated BigQuery copy:
  * schema alignment  - column names, data types, nullability, decimal precision
  * row counts        - with optional filter and tolerance
  * numeric profile   - SUM, MIN, MAX and NULL counts per column
Every mismatch or error is written to an exceptions log (CSV and/or BigQuery).
"""

__version__ = "0.3.0"
