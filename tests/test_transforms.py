"""Transformations and expectation rules, run on local Spark."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shipments import transforms as t
from tests.factories import raw_event

BronzeBuilder = Callable[..., DataFrame]


def passes(df: DataFrame, rule: str) -> list[bool]:
    """Evaluate a rule the way an expectation does: NULL counts as a failure."""
    return [
        r["ok"] for r in df.select(F.coalesce(F.expr(rule), F.lit(False)).alias("ok")).collect()
    ]


class TestBronze:
    def test_add_ingest_metadata_records_source_file(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        landing = tmp_path / "shipments"
        landing.mkdir()
        (landing / "batch-001.json").write_text(json.dumps(raw_event()) + "\n")

        raw = spark.read.schema(t.RAW_SHIPMENT_SCHEMA).json(str(landing))
        row = t.add_ingest_metadata(raw).first()

        assert row is not None
        assert row["_source_file"].endswith("shipments/batch-001.json")
        assert row["_ingested_at"] is not None
        assert row["shipment_id"] == "SHP-00000001"


class TestCleanShipments:
    def test_normalises_and_casts(self, bronze_df: BronzeBuilder) -> None:
        row = t.clean_shipments(
            bronze_df(
                raw_event(
                    shipment_id=" shp-00000001 ",
                    carrier_code="crx",
                    origin_hub=" ams",
                    status=" Delivered ",
                )
            )
        ).first()

        assert row is not None
        assert row["shipment_id"] == "SHP-00000001"
        assert row["carrier_code"] == "CRX"
        assert row["origin_hub"] == "AMS"
        assert row["lane"] == "AMS-BER"
        assert row["status"] == "delivered"
        assert row["weight_kg"] == 2.5
        assert row["delivered_at"] == datetime(2026, 3, 3, 10, 0)

    def test_malformed_values_become_null_instead_of_failing(
        self, bronze_df: BronzeBuilder
    ) -> None:
        row = t.clean_shipments(
            bronze_df(raw_event(weight_kg="12,5", event_ts="not-a-timestamp"))
        ).first()

        assert row is not None
        assert row["weight_kg"] is None
        assert row["event_ts"] is None

    def test_output_columns(self, bronze_df: BronzeBuilder) -> None:
        assert t.clean_shipments(bronze_df(raw_event())).columns == [
            "shipment_id",
            "carrier_code",
            "origin_hub",
            "destination_hub",
            "lane",
            "service_level",
            "status",
            "weight_kg",
            "picked_up_at",
            "promised_by",
            "delivered_at",
            "event_ts",
            "_ingested_at",
            "_source_file",
        ]


class TestExpectationRules:
    @pytest.mark.parametrize(
        ("shipment_id", "expected"),
        [
            ("SHP-00000001", True),
            (" shp-00000002 ", True),  # normalised before the rule runs
            ("SHP-1", False),
            ("ORD-00000001", False),
            (None, False),
        ],
    )
    def test_valid_shipment_id(
        self, bronze_df: BronzeBuilder, shipment_id: str | None, expected: bool
    ) -> None:
        silver = t.clean_shipments(bronze_df(raw_event(shipment_id=shipment_id)))
        assert passes(silver, t.VALID_SHIPMENT_ID) == [expected]

    @pytest.mark.parametrize(
        ("overrides", "failing_rule"),
        [
            ({"event_ts": None}, "valid_event_ts"),
            ({"event_ts": "garbage"}, "valid_event_ts"),
            ({"status": "teleported"}, "known_status"),
            ({"destination_hub": "AMS"}, "valid_lane"),
            ({"origin_hub": "AMSTERDAM"}, "valid_lane"),
        ],
    )
    def test_drop_rules_catch_each_defect(
        self, bronze_df: BronzeBuilder, overrides: dict[str, str | None], failing_rule: str
    ) -> None:
        silver = t.clean_shipments(bronze_df(raw_event(**overrides)))
        results = {name: passes(silver, rule)[0] for name, rule in t.SILVER_DROP_RULES.items()}
        assert results == {name: name != failing_rule for name in t.SILVER_DROP_RULES}

    def test_valid_event_passes_every_rule(self, bronze_df: BronzeBuilder) -> None:
        silver = t.clean_shipments(bronze_df(raw_event()))
        rules = {
            "valid_shipment_id": t.VALID_SHIPMENT_ID,
            **t.SILVER_DROP_RULES,
            **t.SILVER_WARN_RULES,
        }
        assert {name: passes(silver, rule)[0] for name, rule in rules.items()} == dict.fromkeys(
            rules, True
        )

    def test_warn_rules(self, bronze_df: BronzeBuilder) -> None:
        silver = t.clean_shipments(
            bronze_df(
                raw_event(weight_kg="0"),
                raw_event(delivered_at=None),
                raw_event(delivered_at="2026-03-01T09:00:00Z"),
            )
        )
        assert passes(silver, t.SILVER_WARN_RULES["positive_weight"]) == [False, True, True]
        assert passes(silver, t.SILVER_WARN_RULES["delivered_has_timestamp"]) == [True, False, True]
        assert passes(silver, t.SILVER_WARN_RULES["delivery_after_pickup"]) == [True, True, False]


class TestGold:
    def test_latest_event_wins_even_when_it_arrives_first(self, bronze_df: BronzeBuilder) -> None:
        events = bronze_df(
            raw_event(status="delivered", event_ts="2026-03-03T10:00:00Z"),
            raw_event(status="picked_up", delivered_at=None, event_ts="2026-03-01T10:00:00Z"),
        )
        latest = t.latest_event_per_shipment(t.clean_shipments(events)).collect()
        assert [r["status"] for r in latest] == ["delivered"]

    def test_lane_performance_kpis(self, bronze_df: BronzeBuilder) -> None:
        events = bronze_df(
            # on time: 48 h transit, delivered before promise
            raw_event(shipment_id="SHP-00000001"),
            # late: 96 h transit, after promise
            raw_event(
                shipment_id="SHP-00000002",
                delivered_at="2026-03-05T10:00:00Z",
                event_ts="2026-03-05T10:00:00Z",
            ),
            # still moving: counted as a shipment, not in delivery KPIs
            raw_event(shipment_id="SHP-00000003", status="in_transit", delivered_at=None),
            # different lane
            raw_event(
                shipment_id="SHP-00000004", origin_hub="OSL", status="exception", delivered_at=None
            ),
        )
        gold = t.lane_performance(t.latest_event_per_shipment(t.clean_shipments(events)))
        rows = {(r["lane"], r["carrier_code"]): r.asDict() for r in gold.collect()}

        ams_ber = rows[("AMS-BER", "CRX")]
        assert ams_ber["shipments"] == 3
        assert ams_ber["delivered"] == 2
        assert ams_ber["delivered_on_time"] == 1
        assert ams_ber["on_time_rate"] == 0.5
        assert ams_ber["avg_transit_hours"] == 72.0

        osl_ber = rows[("OSL-BER", "CRX")]
        assert osl_ber["exceptions"] == 1
        assert osl_ber["on_time_rate"] is None  # no deliveries yet: unknown, not 0
        assert osl_ber["avg_transit_hours"] is None

    def test_gold_rules_hold_on_computed_output(self, bronze_df: BronzeBuilder) -> None:
        gold = t.lane_performance(
            t.latest_event_per_shipment(t.clean_shipments(bronze_df(raw_event())))
        )
        for rule in t.GOLD_WARN_RULES.values():
            assert passes(gold, rule) == [True]
