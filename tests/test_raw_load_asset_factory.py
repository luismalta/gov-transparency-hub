from typing import ClassVar

import dlt
import duckdb
import pandas as pd
import pytest
from dagster import AssetKey, MultiPartitionKey, materialize
from databricks.sdk.errors import NotFound

from gov_transparency_hub.assets.raw_load_assets import (
    RawTableConfig,
    build_raw_load_assets,
)
from gov_transparency_hub.resources import DatabricksVolumeResource
from gov_transparency_hub.resources.databricks_dlt_resource import (
    DatabricksDltResource,
)

LICITACOES = RawTableConfig(
    "licitacoes", ("municipio", "unidade_gestora", "numero_do_processo")
)
PARTITION = MultiPartitionKey({"date": "2025-01-02", "city": "tiradentes"})
EXPECTED_PATH = (
    "tiradentes/licitacoes/tiradentes-licitacoes-2025-01-02.parquet"
)


class FakeVolumeResource(DatabricksVolumeResource):
    # Resources are pydantic models: test data must be a ClassVar, not a field.
    frames: ClassVar[dict[str, pd.DataFrame]] = {}

    def download_dataframe(self, file_path: str) -> pd.DataFrame:
        if file_path not in self.frames:
            raise NotFound(file_path)
        return self.frames[file_path]


class DuckDbDltResource(DatabricksDltResource):
    duckdb_path: str

    def destination(self):
        return dlt.destinations.duckdb(self.duckdb_path)


@pytest.fixture
def resources(tmp_path):
    FakeVolumeResource.frames = {}
    return {
        "databricks_volume_resource": FakeVolumeResource(
            host="h", token="t", catalog="c", schema_name="s", volume="v"
        ),
        "databricks_dlt_resource": DuckDbDltResource(
            host="h",
            http_path="p",
            token="t",
            catalog="c",
            duckdb_path=str(tmp_path / "warehouse.duckdb"),
        ),
    }


def test_factory_builds_one_asset_per_config():
    other = RawTableConfig("receita", ("municipio",), strategy="delete-insert")

    assets = build_raw_load_assets("raw_reports", [LICITACOES, other])

    assert [a.key for a in assets] == [
        AssetKey("raw_licitacoes"),
        AssetKey("raw_receita"),
    ]
    licitacoes = assets[0]
    assert licitacoes.asset_deps[AssetKey("raw_licitacoes")] == {
        AssetKey("df_licitacoes")
    }
    assert licitacoes.group_names_by_key[AssetKey("raw_licitacoes")] == (
        "raw_reports"
    )
    assert licitacoes.op.pool == "raw_licitacoes"


def test_materialize_loads_parquet_into_table(resources):
    FakeVolumeResource.frames[EXPECTED_PATH] = pd.DataFrame(
        {"numeroDoProcesso": ["1/2025"], "unidade_gestora": ["1"]}
    )
    [asset] = build_raw_load_assets("raw_reports", [LICITACOES])

    result = materialize([asset], resources=resources, partition_key=PARTITION)

    assert result.success
    with duckdb.connect(resources["databricks_dlt_resource"].duckdb_path) as c:
        rows = c.sql(
            "select municipio, ano_referencia, numero_do_processo "
            "from raw.licitacoes"
        ).fetchall()
    assert rows == [("tiradentes", "2025", "1/2025")]
    materialization = result.asset_materializations_for_node("raw_licitacoes")[0]
    assert materialization.metadata["rows"].value == 1
    assert materialization.metadata["table"].value == "c.raw.licitacoes"


def test_empty_parquet_skips_load(resources):
    FakeVolumeResource.frames[EXPECTED_PATH] = pd.DataFrame()
    [asset] = build_raw_load_assets("raw_reports", [LICITACOES])

    result = materialize([asset], resources=resources, partition_key=PARTITION)

    assert result.success
    materialization = result.asset_materializations_for_node("raw_licitacoes")[0]
    assert materialization.metadata["rows"].value == 0


def test_missing_parquet_fails_with_actionable_message(resources):
    [asset] = build_raw_load_assets("raw_reports", [LICITACOES])

    with pytest.raises(FileNotFoundError, match="Materialize df_licitacoes"):
        materialize([asset], resources=resources, partition_key=PARTITION)
