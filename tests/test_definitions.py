from dagster import AssetKey

from gov_transparency_hub import defs


def test_legacy_pipeline_is_gone():
    asset_keys = defs.resolve_asset_graph().get_all_asset_keys()

    assert AssetKey("load_raw_revenue_details") not in asset_keys
    assert AssetKey("revenue_html_report") not in asset_keys
    assert not list(defs.schedules or [])
    assert not list(defs.jobs or [])


def test_portal_and_raw_assets_remain():
    asset_keys = defs.resolve_asset_graph().get_all_asset_keys()

    assert AssetKey("df_despesa") in asset_keys
    assert AssetKey("raw_despesa") in asset_keys


def test_dbt_staging_depends_on_raw_asset():
    graph = defs.resolve_asset_graph()

    # dagster-dbt keys models by [schema, name] when a custom schema is set.
    assert graph.get(AssetKey(["stg", "stg_despesa"])).parent_keys == {
        AssetKey("raw_despesa")
    }


def test_dbt_models_are_grouped_by_layer():
    graph = defs.resolve_asset_graph()

    assert graph.get(AssetKey(["stg", "stg_despesa"])).group_name == "dbt_staging"
    assert graph.get(AssetKey(["curated", "fct_despesa"])).group_name == "curated"
    assert graph.get(AssetKey(["curated", "municipios"])).group_name == "curated"
