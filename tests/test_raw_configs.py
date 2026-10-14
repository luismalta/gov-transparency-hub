from dagster import AssetKey

from gov_transparency_hub import defs
from gov_transparency_hub.assets.df_reports import DF_REPORT_ENDPOINTS
from gov_transparency_hub.assets.df_resources import DF_RESOURCE_ENDPOINTS
from gov_transparency_hub.assets.raw_reports import RAW_REPORT_CONFIGS
from gov_transparency_hub.assets.raw_resources import RAW_RESOURCE_CONFIGS


def test_every_report_endpoint_has_a_raw_config():
    assert {c.endpoint for c in RAW_REPORT_CONFIGS} == {
        e.endpoint for e in DF_REPORT_ENDPOINTS
    }


def test_every_resource_endpoint_has_a_raw_config():
    assert {c.endpoint for c in RAW_RESOURCE_CONFIGS} == {
        e.endpoint for e in DF_RESOURCE_ENDPOINTS
    }


def test_tables_without_natural_key_use_delete_insert():
    configs = {c.endpoint: c for c in RAW_REPORT_CONFIGS + RAW_RESOURCE_CONFIGS}

    assert configs["receita"].strategy == "delete-insert"
    assert configs["tipos_de_despesas"].strategy == "delete-insert"
    upserts = [c for c in configs.values() if c.strategy == "upsert"]
    assert len(upserts) == 20


def test_raw_assets_are_registered_in_definitions():
    asset_keys = defs.resolve_asset_graph().get_all_asset_keys()

    assert AssetKey("raw_despesa") in asset_keys
    assert AssetKey("raw_tipos_de_despesas") in asset_keys


def test_portal_assets_use_df_prefix():
    asset_keys = defs.resolve_asset_graph().get_all_asset_keys()

    assert AssetKey("df_despesa") in asset_keys
    assert AssetKey("df_tipos_de_despesas") in asset_keys
    assert AssetKey("despesa") not in asset_keys


def test_df_and_raw_assets_are_wired():
    graph = defs.resolve_asset_graph()

    assert AssetKey("df_unidades_gestoras") in graph.get(
        AssetKey("df_despesa")
    ).parent_keys
    assert graph.get(AssetKey("raw_despesa")).parent_keys == {
        AssetKey("df_despesa")
    }


def test_portal_assets_use_df_groups():
    graph = defs.resolve_asset_graph()

    assert graph.get(AssetKey("df_despesa")).group_name == "df_reports"
    assert graph.get(AssetKey("df_funcoes")).group_name == "df_resources"
