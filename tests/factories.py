"""Builders for synthetic raw shipment events used across tests."""

from __future__ import annotations


def raw_event(**overrides: str | None) -> dict[str, str | None]:
    """A valid raw event; override any field to build edge cases."""
    event: dict[str, str | None] = {
        "shipment_id": "SHP-00000001",
        "carrier_code": "CRX",
        "origin_hub": "AMS",
        "destination_hub": "BER",
        "service_level": "standard",
        "status": "delivered",
        "weight_kg": "2.50",
        "picked_up_at": "2026-03-01T10:00:00Z",
        "promised_by": "2026-03-04T10:00:00Z",
        "delivered_at": "2026-03-03T10:00:00Z",
        "event_ts": "2026-03-03T10:00:00Z",
    }
    event.update(overrides)
    return event
