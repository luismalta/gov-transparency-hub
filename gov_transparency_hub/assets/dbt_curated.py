from collections.abc import Mapping
from typing import Any

from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from gov_transparency_hub.resources.dbt_connection import dbt_project


class LayerDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        if "staging" in dbt_resource_props["fqn"]:
            return "dbt_staging"
        return "curated"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=LayerDbtTranslator(),
)
def dbt_curated_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
