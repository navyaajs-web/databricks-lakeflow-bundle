"""Shared fixtures: a small local SparkSession and a row builder."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator

import pytest
from pyspark.sql import DataFrame, SparkSession

from shipments.transforms import RAW_SHIPMENT_SCHEMA

# PySpark converts timestamps to naive datetimes in the Python process time
# zone when collecting; pin everything to UTC so results are machine-independent.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("shipments-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def bronze_df(spark: SparkSession) -> Callable[..., DataFrame]:
    """Build a bronze-shaped DataFrame (raw schema + lineage columns) from dicts."""

    def build(
        *events: dict[str, str | None], ingested_at: str = "2026-03-05 00:00:00"
    ) -> DataFrame:
        df = spark.createDataFrame(list(events), schema=RAW_SHIPMENT_SCHEMA)
        return df.selectExpr(
            "*",
            f"TIMESTAMP'{ingested_at}' AS _ingested_at",
            "'file:/test/landing.json' AS _source_file",
        )

    return build
