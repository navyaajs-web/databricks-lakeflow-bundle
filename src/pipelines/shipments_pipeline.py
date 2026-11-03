"""Lakeflow Spark Declarative Pipeline: shipments bronze -> silver -> gold.

This file only declares datasets and their expectations. All transformation
logic lives in `shipments.transforms` (plain PySpark, unit-tested locally).
The import works because the bundle sets the pipeline's `root_path` to `src/`,
which the pipeline runtime adds to `sys.path`.

Decorator API: `from pyspark import pipelines as dp` is the current import
(it is what `databricks bundle init` templates generate). The older
`import dlt` module offers the same decorators (`@dlt.table`,
`@dp.expect_or_drop`, ...) and still works; switching is a rename.
"""

from __future__ import annotations

from pyspark import pipelines as dp  # resolved by the Databricks pipeline runtime
from pyspark.sql import DataFrame, SparkSession

from shipments import transforms as t

# The runtime injects `spark`; fetching the active session explicitly keeps
# linters and readers from wondering where it comes from.
spark = SparkSession.getActiveSession()

# Set in resources/shipments.pipeline.yml from bundle variables, so the same
# code reads /Volumes/<catalog>/<schema>/landing/shipments in dev and prod.
LANDING_PATH = spark.conf.get("shipments.landing_path")


@dp.table(
    name="bronze_shipments",
    comment="Raw shipment events from the landing volume, as received, plus lineage columns.",
    table_properties={"quality": "bronze"},
)
def bronze_shipments() -> DataFrame:
    # Auto Loader tracks which files it has already processed, so each update
    # only reads new files. An explicit schema avoids inference drift; fields
    # the schema does not know are kept in _rescued_data.
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(t.RAW_SHIPMENT_SCHEMA)
        .load(LANDING_PATH)
    )
    return t.add_ingest_metadata(raw)


@dp.table(
    name="silver_shipments",
    comment="Typed, validated shipment events. Rows breaking a drop rule are removed and counted.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_shipment_id", t.VALID_SHIPMENT_ID)
@dp.expect_all_or_drop(t.SILVER_DROP_RULES)
@dp.expect_all(t.SILVER_WARN_RULES)
def silver_shipments() -> DataFrame:
    return t.clean_shipments(spark.readStream.table("bronze_shipments"))


@dp.materialized_view(
    name="gold_lane_performance",
    comment="On-time rate, volume and transit time per lane and carrier (latest shipment state).",
    table_properties={"quality": "gold"},
)
@dp.expect_all(t.GOLD_WARN_RULES)
def gold_lane_performance() -> DataFrame:
    # A materialized view over a batch read: recomputed (incrementally where
    # the engine can) from silver on every pipeline update.
    return t.lane_performance(t.latest_event_per_shipment(spark.read.table("silver_shipments")))
