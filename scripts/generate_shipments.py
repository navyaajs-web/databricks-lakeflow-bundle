"""Generate synthetic shipment events as JSON lines.

Fictional data only: random hubs, carriers and ids, seeded for reproducibility.
A small share of events is deliberately broken (bad ids, unknown statuses,
non-numeric weights) so the pipeline's expectations have something to catch.

    python scripts/generate_shipments.py --shipments 500 --out data/shipments.json
"""

from __future__ import annotations

import argparse
import json
import random
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

Event = dict[str, str | None]

HUBS = ("AMS", "BER", "CPH", "DUB", "LIS", "MAD", "OSL", "PRG", "VIE", "WAW")
# Made-up carrier codes; deliberately not real companies.
CARRIERS = ("CRX", "PQL", "ZVT")
SERVICE_HOURS = {"express": 24, "standard": 72, "economy": 120}


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def shipment_events(rng: random.Random, number: int, start: datetime) -> Iterator[Event]:
    """Yield the status events of one shipment, oldest first."""
    origin, destination = rng.sample(HUBS, 2)
    service = rng.choice(tuple(SERVICE_HOURS))
    created = start + timedelta(minutes=rng.randint(0, 24 * 60))
    picked_up = created + timedelta(hours=rng.uniform(1, 6))
    promised = picked_up + timedelta(hours=SERVICE_HOURS[service])
    # Most parcels arrive on time; some are late, a few never arrive.
    transit = timedelta(hours=SERVICE_HOURS[service] * rng.uniform(0.5, 1.4))
    delivered = picked_up + transit
    outcome = rng.choices(("delivered", "in_transit", "exception"), weights=(80, 15, 5))[0]
    carrier = rng.choice(CARRIERS)
    # Some producers send lowercase codes; silver normalises them.
    if rng.random() < 0.1:
        carrier = carrier.lower()

    base = {
        "shipment_id": f"SHP-{number:08d}",
        "carrier_code": carrier,
        "origin_hub": origin,
        "destination_hub": destination,
        "service_level": service,
        "weight_kg": f"{rng.uniform(0.2, 30):.2f}",
        "picked_up_at": None,
        "promised_by": _iso(promised),
        "delivered_at": None,
    }
    yield {**base, "status": "created", "event_ts": _iso(created)}
    picked = {**base, "picked_up_at": _iso(picked_up)}
    yield {**picked, "status": "picked_up", "event_ts": _iso(picked_up)}
    if outcome == "delivered":
        yield {
            **base,
            "status": "delivered",
            "picked_up_at": _iso(picked_up),
            "delivered_at": _iso(delivered),
            "event_ts": _iso(delivered),
        }
    else:
        mid = picked_up + transit / 2
        yield {**base, "status": outcome, "picked_up_at": _iso(picked_up), "event_ts": _iso(mid)}


def corrupt(rng: random.Random, event: Event) -> Event:
    """Return a copy of `event` with one realistic defect."""
    broken = dict(event)
    defect = rng.choice(("id", "status", "weight", "lane", "ts"))
    if defect == "id":
        broken["shipment_id"] = "shp_" + str(rng.randint(1, 999))
    elif defect == "status":
        broken["status"] = "teleported"
    elif defect == "weight":
        broken["weight_kg"] = "12,5"
    elif defect == "lane":
        broken["destination_hub"] = broken["origin_hub"]
    else:
        broken["event_ts"] = "not-a-timestamp"
    return broken


def generate(shipments: int, seed: int = 7, error_rate: float = 0.03) -> list[Event]:
    rng = random.Random(seed)
    start = datetime(2026, 3, 1, tzinfo=UTC)
    events: list[Event] = []
    for number in range(1, shipments + 1):
        for event in shipment_events(rng, number, start):
            events.append(corrupt(rng, event) if rng.random() < error_rate else event)
    rng.shuffle(events)  # files rarely arrive in event order
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--shipments", type=int, default=200)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--out", type=Path, default=Path("data/shipments.json"))
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    events = generate(args.shipments, args.seed)
    args.out.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    print(f"wrote {len(events)} events to {args.out}")


if __name__ == "__main__":
    main()
