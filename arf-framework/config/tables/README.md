Put one YAML file per table here (copy `../templates/table_template.yaml`).

A single file can also hold many tables:

```yaml
tables:
  - table_id: orders
    source: {connection: databricks_legacy, table: main.sales.orders}
    target: {connection: bigquery_target, table: my-project.sales.orders}
  - table_id: customers
    source: {connection: databricks_legacy, table: main.sales.customers}
    target: {connection: bigquery_target, table: my-project.sales.customers}
```
