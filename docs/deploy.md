# Deploying the bundle (optional)

CI tests the transformations and the bundle configuration without a workspace. This page covers deploying to **your own** Databricks workspace.
A Databricks Free Edition workspace is enough for the `dev` target.

## 1. Prerequisites

- A recent Databricks CLI. The configuration was validated with v1.23.0 (`databricks --version`).
  [Install guide](https://docs.databricks.com/dev-tools/cli/install.html).
- A workspace with Unity Catalog and serverless compute. Free Edition workspaces have both, plus:
  - a `workspace` catalog;
  - a SQL warehouse named `Serverless Starter Warehouse`.
- Authenticate with OAuth. A browser login stores a short-lived token in your local profile:
  ```bash
  databricks auth login --host https://<your-workspace>.cloud.databricks.com
  ```
  Avoid personal access tokens where you can. Never commit `~/.databrickscfg`, `.env` or tokens.

## 2. Deploy to dev

```bash
make validate                     # databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Development mode does three things:
- Prefixes names: the pipeline becomes `[dev <you>] shipments-pipeline`, and the schema becomes `dev_<you>_shipments`.
- Pauses the job schedule.
- Runs the pipeline in development mode, so clusters are reused and retries are off.

Several engineers can deploy to the same workspace without clashing.

If your SQL warehouse has a different name, pass its ID:

```bash
databricks bundle deploy -t dev --var warehouse_id=<warehouse-id>
```

## 3. Land synthetic data and run

```bash
make sample-data   # writes data/shipments.json (fictional events, some deliberately invalid)

SCHEMA=dev_$(databricks current-user me --output json | jq -r '.userName | split("@")[0] | gsub("[^a-zA-Z0-9]"; "_")')_shipments
databricks fs cp data/shipments.json \
  "dbfs:/Volumes/workspace/${SCHEMA}/landing/shipments/batch-0001.json"

databricks bundle run -t dev shipments_daily
```

The schema prefix is derived from your user name. If the `cp` path is wrong, check the actual schema name in Catalog Explorer.

The pipeline's event log and data quality tab show how many rows each expectation dropped or flagged. Once the run finishes, query the results:

```sql
SELECT * FROM workspace.dev_<you>_shipments.gold_lane_performance ORDER BY shipments DESC;
SELECT * FROM workspace.dev_<you>_shipments.lane_kpi_history;
```

## 4. Promote to prod

Run `prod` from CI, not from a laptop:

1. Create a service principal. Grant it the following:
   - `USE CATALOG` and `CREATE SCHEMA` on the prod catalog (`logistics_prod` by default; override with `--var catalog=...`);
   - `CAN USE` on the SQL warehouse.
2. Give CI the service principal's OAuth credentials as secrets: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`.
3. Deploy:
   ```bash
   databricks bundle deploy -t prod --var prod_run_as=<service-principal-application-id>
   ```

Production mode keeps the daily schedule (05:30 UTC) unpaused. The job and pipeline run as the service principal. Bundle files live in the service principal's workspace folder.

## 5. Unity Catalog grants (documented, not applied)

The bundle creates the schema and the landing volume but does not manage grants. Grants usually belong to a central access-management process. Suggested grants for prod:

```sql
-- Producers write raw files only.
GRANT USE CATALOG ON CATALOG logistics_prod TO `shipments-producers`;
GRANT USE SCHEMA ON SCHEMA logistics_prod.shipments TO `shipments-producers`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME logistics_prod.shipments.landing TO `shipments-producers`;

-- Analysts read curated tables only.
GRANT USE CATALOG ON CATALOG logistics_prod TO `logistics-analysts`;
GRANT USE SCHEMA ON SCHEMA logistics_prod.shipments TO `logistics-analysts`;
GRANT SELECT ON TABLE logistics_prod.shipments.gold_lane_performance TO `logistics-analysts`;
GRANT SELECT ON TABLE logistics_prod.shipments.lane_kpi_history TO `logistics-analysts`;
```

Bronze and silver stay restricted to the pipeline owner. Bronze keeps raw payloads, including `_rescued_data`.

## 6. Cost notes

- The pipeline is **serverless and triggered**. It costs DBUs only while an update runs, about a few minutes per day at sample-data scale. Nothing runs in `dev` unless you start it, because development mode pauses the schedule.
- The SQL task uses a serverless SQL warehouse. Set a short auto-stop on it, for example 5-10 minutes.
- The free quota of a Free Edition workspace easily covers the sample data.
- To remove everything the bundle created, run `databricks bundle destroy -t dev`. This deletes the schema, and with it the tables and the volume.

## 7. Security notes

- No workspace host, token, or user name is committed. Configuration comes from environment variables or the CLI profile.
- The CI `bundle-validate` job runs only when a `DATABRICKS_HOST` secret exists, so forks never receive credentials.
- Use a dedicated, least-privileged service principal for prod. Do not deploy prod with a personal identity.
- The pipeline keeps lineage columns `_source_file` and `_ingested_at`, so every row can be traced to its landing file.
