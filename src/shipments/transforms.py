"""Plain PySpark transformations for the shipments pipeline.

Nothing in this module imports `dlt` or touches Databricks-only APIs, so every
function runs on a local SparkSession under pytest. The pipeline definition
(src/pipelines/shipments_pipeline.py) is a thin layer that wires these
functions to tables and attaches the data quality expectations defined here.

Domain (fictional): a parcel network emits one JSON event per shipment status
change. Each event carries the full current state of the shipment.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------
# Raw schema
# ---------------------------------------------------------------------------

#: Schema applied by Auto Loader. Everything is read as a string and cast in
#: the silver layer: a producer sending "12,5" for a weight should produce a
#: NULL that an expectation reports, not a failed stream. Unknown fields land
#: in Auto Loader's _rescued_data column instead of being dropped silently.
RAW_SHIPMENT_SCHEMA = T.StructType(
    [
        T.StructField("shipment_id", T.StringType()),
        T.StructField("carrier_code", T.StringType()),
        T.StructField("origin_hub", T.StringType()),
        T.StructField("destination_hub", T.StringType()),
        T.StructField("service_level", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("weight_kg", T.StringType()),
        T.StructField("picked_up_at", T.StringType()),
        T.StructField("promised_by", T.StringType()),
        T.StructField("delivered_at", T.StringType()),
        T.StructField("event_ts", T.StringType()),
    ]
)

KNOWN_STATUSES: tuple[str, ...] = (
    "created",
    "picked_up",
    "in_transit",
    "out_for_delivery",
    "delivered",
    "exception",
    "returned",
)

# ---------------------------------------------------------------------------
# Data quality rules (SQL boolean expressions, evaluated on silver rows)
#
# Kept here, not in the pipeline file, so tests can evaluate the exact same
# expressions with local Spark.
# ---------------------------------------------------------------------------

#: A shipment id that cannot be joined or deduplicated is useless downstream.
VALID_SHIPMENT_ID = "shipment_id IS NOT NULL AND shipment_id RLIKE '^SHP-[0-9]{8}$'"

#: Rows failing any of these are dropped (and counted in the pipeline event log).
SILVER_DROP_RULES: dict[str, str] = {
    "valid_event_ts": "event_ts IS NOT NULL",
    "known_status": "status IN ({})".format(", ".join(f"'{s}'" for s in KNOWN_STATUSES)),
    "valid_lane": (
        "origin_hub RLIKE '^[A-Z]{3}$' AND destination_hub RLIKE '^[A-Z]{3}$' "
        "AND origin_hub <> destination_hub"
    ),
}

#: Rows failing these are kept but counted, so the trend is visible without
#: losing data that is merely suspicious.
SILVER_WARN_RULES: dict[str, str] = {
    "positive_weight": "weight_kg > 0",
    "delivered_has_timestamp": "status <> 'delivered' OR delivered_at IS NOT NULL",
    "delivery_after_pickup": (
        "delivered_at IS NULL OR picked_up_at IS NULL OR delivered_at >= picked_up_at"
    ),
}

#: Sanity checks on the gold aggregate.
GOLD_WARN_RULES: dict[str, str] = {
    "on_time_rate_in_range": "on_time_rate IS NULL OR on_time_rate BETWEEN 0 AND 1",
    "non_negative_transit": "avg_transit_hours IS NULL OR avg_transit_hours >= 0",
}


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------


def add_ingest_metadata(raw: DataFrame) -> DataFrame:
    """Add lineage columns to a file-based source.

    `_metadata.file_path` is Spark's hidden file-source column (also exposed
    by Auto Loader). Keeping it in bronze makes it possible to trace any bad
    row back to the exact landing file.
    """
    return raw.select(
        "*",
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file"),
    )


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------


def _ts(column: str) -> F.Column:
    # try_cast returns NULL for malformed input regardless of ANSI mode, which
    # is on by default in Spark 4 / serverless and would otherwise fail the
    # stream on a single bad value.
    return F.expr(f"try_cast({column} AS timestamp)")


def _upper_code(column: str) -> F.Column:
    return F.upper(F.trim(F.col(column)))


def clean_shipments(bronze: DataFrame) -> DataFrame:
    """Normalise codes, cast types and derive the lane.

    Pure column logic only; filtering is left to the expectations so that
    dropped rows are counted and reported by the pipeline.
    """
    origin = _upper_code("origin_hub")
    destination = _upper_code("destination_hub")
    return bronze.select(
        _upper_code("shipment_id").alias("shipment_id"),
        _upper_code("carrier_code").alias("carrier_code"),
        origin.alias("origin_hub"),
        destination.alias("destination_hub"),
        F.concat_ws("-", origin, destination).alias("lane"),
        F.lower(F.trim(F.col("service_level"))).alias("service_level"),
        F.lower(F.trim(F.col("status"))).alias("status"),
        F.expr("try_cast(trim(weight_kg) AS double)").alias("weight_kg"),
        _ts("picked_up_at").alias("picked_up_at"),
        _ts("promised_by").alias("promised_by"),
        _ts("delivered_at").alias("delivered_at"),
        _ts("event_ts").alias("event_ts"),
        F.col("_ingested_at"),
        F.col("_source_file"),
    )


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------


def latest_event_per_shipment(silver: DataFrame) -> DataFrame:
    """Keep the most recent event for each shipment.

    Events can arrive late and out of order, so "latest" is decided by the
    event timestamp, with ingestion time as a deterministic tie-breaker.
    """
    window = Window.partitionBy("shipment_id").orderBy(
        F.col("event_ts").desc(), F.col("_ingested_at").desc()
    )
    return (
        silver.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")
    )


def lane_performance(latest: DataFrame) -> DataFrame:
    """Aggregate per lane and carrier: volume, delivery and on-time KPIs.

    `on_time_rate` is computed over delivered shipments only; it is NULL for a
    lane with no deliveries yet rather than a misleading 0.
    """
    delivered = F.col("status") == "delivered"
    on_time = delivered & (F.col("delivered_at") <= F.col("promised_by"))
    transit_hours = (F.unix_timestamp("delivered_at") - F.unix_timestamp("picked_up_at")) / F.lit(
        3600.0
    )

    aggregated = latest.groupBy("lane", "carrier_code").agg(
        F.count(F.lit(1)).alias("shipments"),
        F.count(F.when(delivered, True)).alias("delivered"),
        F.count(F.when(on_time, True)).alias("delivered_on_time"),
        F.count(F.when(F.col("status") == "exception", True)).alias("exceptions"),
        F.avg(F.when(delivered, transit_hours)).alias("avg_transit_hours"),
    )
    return aggregated.select(
        "lane",
        "carrier_code",
        "shipments",
        "delivered",
        "delivered_on_time",
        "exceptions",
        F.when(
            F.col("delivered") > 0,
            F.round(F.col("delivered_on_time") / F.col("delivered"), 4),
        ).alias("on_time_rate"),
        F.round("avg_transit_hours", 2).alias("avg_transit_hours"),
    )
