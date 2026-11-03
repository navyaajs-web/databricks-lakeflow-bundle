"""Synthetic data -> bronze -> silver (with drop rules) -> gold, all locally.

This mirrors what the pipeline does on Databricks, minus Auto Loader and the
expectation bookkeeping: drop rules are applied as plain filters here.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from scripts.generate_shipments import generate
from shipments import transforms as t


def test_pipeline_logic_on_synthetic_data(spark: SparkSession, tmp_path: Path) -> None:
    events = generate(shipments=150, seed=11, error_rate=0.05)
    landing = tmp_path / "landing" / "shipments"
    landing.mkdir(parents=True)
    (landing / "batch-001.json").write_text("\n".join(json.dumps(e) for e in events))

    bronze = t.add_ingest_metadata(spark.read.schema(t.RAW_SHIPMENT_SCHEMA).json(str(landing)))
    silver_all = t.clean_shipments(bronze)
    keep = F.expr(t.VALID_SHIPMENT_ID)
    for rule in t.SILVER_DROP_RULES.values():
        keep = keep & F.coalesce(F.expr(rule), F.lit(False))
    silver = silver_all.where(keep)

    assert bronze.count() == len(events)
    dropped = bronze.count() - silver.count()
    assert 0 < dropped < len(events) * 0.1, (
        "the generator's defects should be caught, and only those"
    )

    gold = t.lane_performance(t.latest_event_per_shipment(silver))
    totals = gold.agg(
        F.sum("shipments").alias("n"), F.max("on_time_rate").alias("max_rate")
    ).first()
    assert totals is not None
    assert 0 < totals["n"] <= 150
    assert 0 <= totals["max_rate"] <= 1
    assert gold.where("carrier_code <> upper(carrier_code)").count() == 0
