from typing import ClassVar

import dlt
import duckdb
import pandas as pd
import pytest
from dagster import AssetKey, BackfillPolicy, MultiPartitionKey, materialize
from databricks.sdk.errors import NotFound

from gov_transparency_hub.assets.portal_api_assets import PortalEndpoint
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
TIPOS_DE_EMPENHOS = RawTableConfig("tipos_de_empenhos", ("municipio", "id"))
ENDPOINTS = [
    PortalEndpoint(
        "licitacoes",
        "licitacoes",
        ("unidade_gestora", "exercicio_de_criacao", "mes_de_criacao"),
    ),
    PortalEndpoint("receita", "receitas", ("exercicio", "unidade_gestora")),
    PortalEndpoint("tipos_de_empenhos", "tiposDeEmpenhos"),
]
# licitacoes is partitioned by month (exercicio_de_criacao + mes_de_criacao).
PARTITION = MultiPartitionKey({"month": "2025-01", "city": "tiradentes"})
EXPECTED_PATH = "tiradentes/licitacoes/tiradentes-licitacoes-2025-01.parquet"


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

    assets = build_raw_load_assets("raw_reports", [LICITACOES, other], ENDPOINTS)

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
    [asset] = build_raw_load_assets("raw_reports", [LICITACOES], ENDPOINTS)

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
    [asset] = build_raw_load_assets("raw_reports", [LICITACOES], ENDPOINTS)

    result = materialize([asset], resources=resources, partition_key=PARTITION)

    assert result.success
    materialization = result.asset_materializations_for_node("raw_licitacoes")[0]
    assert materialization.metadata["rows"].value == 0


def test_missing_parquet_fails_with_actionable_message(resources):
    [asset] = build_raw_load_assets("raw_reports", [LICITACOES], ENDPOINTS)

    with pytest.raises(FileNotFoundError, match="Materialize df_licitacoes"):
        materialize([asset], resources=resources, partition_key=PARTITION)


# Adjacent cities in CITY_LIST, so the multi-partition range covers exactly two.
RANGE_TAGS = {
    "dagster/asset_partition_range_start": "tiradentes|2025-01",
    "dagster/asset_partition_range_end": "saojoaodelrei|2025-01",
}
SAOJOAO_PATH = "saojoaodelrei/licitacoes/saojoaodelrei-licitacoes-2025-01.parquet"


def test_raw_assets_backfill_in_a_single_run():
    [asset] = build_raw_load_assets("raw_reports", [LICITACOES], ENDPOINTS)

    assert asset.backfill_policy == BackfillPolicy.single_run()


def test_partition_range_is_loaded_once(resources):
    FakeVolumeResource.frames[EXPECTED_PATH] = pd.DataFrame(
        {"numeroDoProcesso": ["1/2025"], "unidade_gestora": ["1"]}
    )
    FakeVolumeResource.frames[SAOJOAO_PATH] = pd.DataFrame(
        {"numeroDoProcesso": ["7/2025"], "unidade_gestora": ["1"]}
    )
    [asset] = build_raw_load_assets("raw_reports", [LICITACOES], ENDPOINTS)

    result = materialize([asset], resources=resources, tags=RANGE_TAGS)

    assert result.success
    with duckdb.connect(resources["databricks_dlt_resource"].duckdb_path) as c:
        cities = c.sql(
            "select municipio from raw.licitacoes order by 1"
        ).fetchall()
        loads = c.sql("select count(*) from raw._dlt_loads").fetchone()[0]
    assert cities == [("saojoaodelrei",), ("tiradentes",)]
    assert loads == 1


def test_missing_partition_fails_the_batch(resources):
    FakeVolumeResource.frames[EXPECTED_PATH] = pd.DataFrame(
        {"numeroDoProcesso": ["1/2025"], "unidade_gestora": ["1"]}
    )
    [asset] = build_raw_load_assets("raw_reports", [LICITACOES], ENDPOINTS)

    with pytest.raises(FileNotFoundError, match="saojoaodelrei"):
        materialize([asset], resources=resources, tags=RANGE_TAGS)


def test_city_only_resource_has_null_ano_referencia(resources):
    path = "tiradentes/tipos_de_empenhos/tiradentes-tipos_de_empenhos.parquet"
    FakeVolumeResource.frames[path] = pd.DataFrame({"id": ["1"], "nome": ["EO"]})
    [asset] = build_raw_load_assets("raw_resources", [TIPOS_DE_EMPENHOS], ENDPOINTS)

    result = materialize([asset], resources=resources, partition_key="tiradentes")

    assert result.success
    with duckdb.connect(resources["databricks_dlt_resource"].duckdb_path) as c:
        rows = c.sql(
            "select municipio, id from raw.tipos_de_empenhos"
        ).fetchall()
        columns = {r[0] for r in c.sql("describe raw.tipos_de_empenhos").fetchall()}
    assert rows == [("tiradentes", "1")]
    # Time-less cadastro: the column is null (dlt omits an all-null column).
    if "ano_referencia" in columns:
        with duckdb.connect(resources["databricks_dlt_resource"].duckdb_path) as c:
            assert c.sql("select ano_referencia from raw.tipos_de_empenhos").fetchall() == [(None,)]


def test_raw_assets_share_their_df_partitions():
    from gov_transparency_hub import defs

    graph = defs.resolve_asset_graph()
    raw_keys = [k for k in graph.get_all_asset_keys() if k.path[-1].startswith("raw_")]

    assert raw_keys
    for key in raw_keys:
        df_key = AssetKey(key.path[-1].replace("raw_", "df_", 1))
        assert graph.get(key).partitions_def == graph.get(df_key).partitions_def, key
