"""Static checks on the bundle configuration and the pipeline source.

`databricks bundle validate` needs a workspace, so CI only runs it when a host
is configured. These tests catch the common mistakes (missing targets, broken
relative paths, dropped expectations) without one.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
PIPELINE_FILE = ROOT / "src" / "pipelines" / "shipments_pipeline.py"


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


@pytest.fixture(scope="module")
def bundle() -> dict[str, Any]:
    return load_yaml(ROOT / "databricks.yml")


@pytest.fixture(scope="module")
def resources() -> dict[str, dict[str, Any]]:
    """Merge every resources/*.yml the way `include:` does, remembering each file."""
    merged: dict[str, dict[str, Any]] = {}
    for path in sorted((ROOT / "resources").glob("*.yml")):
        for kind, items in load_yaml(path)["resources"].items():
            for name, body in items.items():
                assert name not in merged.setdefault(kind, {}), f"duplicate {kind}.{name}"
                merged[kind][name] = {**body, "__file__": path}
    return merged


def test_targets_and_modes(bundle: dict[str, Any]) -> None:
    targets = bundle["targets"]
    assert set(targets) == {"dev", "prod"}
    assert targets["dev"]["mode"] == "development" and targets["dev"]["default"] is True
    assert targets["prod"]["mode"] == "production"
    assert "service_principal_name" in targets["prod"]["run_as"]
    assert "host" not in targets["prod"].get("workspace", {}), (
        "hosts come from the environment, not git"
    )


def test_unity_catalog_variables(bundle: dict[str, Any]) -> None:
    variables = bundle["variables"]
    assert {"catalog", "schema", "warehouse_id"} <= set(variables)
    assert all(v.get("description") for v in variables.values())
    assert bundle["targets"]["prod"]["variables"]["catalog"] != variables["catalog"]["default"]


def test_include_covers_resources(bundle: dict[str, Any]) -> None:
    assert bundle["include"] == ["resources/*.yml"]


def test_pipeline_definition(resources: dict[str, dict[str, Any]]) -> None:
    pipeline = resources["pipelines"]["shipments_pipeline"]
    assert pipeline["catalog"] == "${var.catalog}"
    assert pipeline["schema"] == "${resources.schemas.shipments.name}"
    assert pipeline["serverless"] is True
    assert "target" not in pipeline, "use `schema`; `target` is the deprecated field"
    assert pipeline["configuration"]["shipments.landing_path"].startswith(
        "/Volumes/${var.catalog}/"
    )

    base = pipeline["__file__"].parent
    for library in pipeline["libraries"]:
        assert (base / library["file"]["path"]).resolve().is_file()
    root_path = (base / pipeline["root_path"]).resolve()
    assert (root_path / "shipments" / "transforms.py").is_file(), (
        "root_path must make `shipments` importable"
    )


def test_job_runs_pipeline_then_sql(resources: dict[str, dict[str, Any]]) -> None:
    job = resources["jobs"]["shipments_daily"]
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert (
        tasks["update_pipeline"]["pipeline_task"]["pipeline_id"]
        == "${resources.pipelines.shipments_pipeline.id}"
    )

    sql = tasks["refresh_lane_kpis"]
    assert sql["depends_on"] == [{"task_key": "update_pipeline"}]
    assert sql["sql_task"]["warehouse_id"] == "${var.warehouse_id}"
    sql_path = (job["__file__"].parent / sql["sql_task"]["file"]["path"]).resolve()
    assert sql_path.is_file()

    # Every named parameter marker used in the SQL file must be supplied.
    text = sql_path.read_text(encoding="utf-8")
    for parameter in ("catalog", "schema"):
        assert f":{parameter}" in text
        assert parameter in sql["sql_task"]["parameters"]

    assert job["max_concurrent_runs"] == 1
    assert job["schedule"]["pause_status"] == "PAUSED"


def _decorators(path: Path) -> dict[str, list[str]]:
    """Map each function in the pipeline file to its decorator names (e.g. 'dp.table')."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: dict[str, list[str]] = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            names = []
            for dec in node.decorator_list:
                target = dec.func if isinstance(dec, ast.Call) else dec
                names.append(ast.unparse(target))
            found[node.name] = names
    return found


def test_pipeline_declares_tables_with_expectations() -> None:
    decorators = _decorators(PIPELINE_FILE)

    assert set(decorators) == {"bronze_shipments", "silver_shipments", "gold_lane_performance"}
    assert "dp.table" in decorators["bronze_shipments"]  # streaming table (Auto Loader)
    assert "dp.table" in decorators["silver_shipments"]  # streaming table
    assert "dp.materialized_view" in decorators["gold_lane_performance"]
    assert "dp.expect_or_drop" in decorators["silver_shipments"]
    assert "dp.expect_all_or_drop" in decorators["silver_shipments"]
    assert "dp.expect_all" in decorators["gold_lane_performance"]


def test_pipeline_file_keeps_logic_in_transforms() -> None:
    source = PIPELINE_FILE.read_text(encoding="utf-8")
    assert "from shipments import transforms" in source
    assert "cloudFiles" in source, "bronze must use Auto Loader"
