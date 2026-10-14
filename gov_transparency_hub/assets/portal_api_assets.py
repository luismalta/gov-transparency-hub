import json
from typing import NamedTuple

import pandas as pd
from dagster import (
    AssetDep,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    Output,
    asset,
)
from databricks.sdk.errors import NotFound

from gov_transparency_hub.assets.utils import format_object_name
from gov_transparency_hub.partitions import partition_scope, partitions_def_for
from gov_transparency_hub.resources import DatabricksVolumeResource
from gov_transparency_hub.resources.constants import URLS
from gov_transparency_hub.resources.portal_transparencia_api import TIMEOUT, fetch


class PortalEndpoint(NamedTuple):
    endpoint: str
    # Listed explicitly because the API does not derive it consistently from the
    # endpoint name (e.g. co_tce -> codigosTce).
    result_key: str
    param_names: tuple[str, ...] = ()
    timeout: int = TIMEOUT


def _partition_params(param_names: tuple[str, ...], period: str | None) -> dict:
    if period is None:
        return {}
    values = {
        "exercicio": period[:4],
        "data_do_empenho_inicial": period,
        "data_do_empenho_final": period,
        "exercicio_de_criacao": period[:4],
        "mes_de_criacao": period[5:7],
    }
    return {name: values[name] for name in param_names if name != "unidade_gestora"}


def _get_portal_data(
    api: str, city_name: str, endpoint: str, params: dict | None, timeout: int
):
    url = f"{URLS.get(city_name)}/api/{api}/{endpoint}"
    return fetch(url, params, timeout), url


def serialize_nested_columns(data_df: pd.DataFrame) -> pd.DataFrame:
    # Raw layer keeps nested objects as JSON strings; parsing happens downstream
    # (e.g. from_json in Databricks).
    for column in data_df.columns:
        if data_df[column].map(lambda value: isinstance(value, (dict, list))).any():
            data_df[column] = data_df[column].map(
                lambda value: json.dumps(value, ensure_ascii=False)
                if isinstance(value, (dict, list))
                else value
            )
    return data_df


def df_asset_name(endpoint: str) -> str:
    # Asset key only: Volume paths stay keyed by the bare endpoint name.
    return f"df_{endpoint}"


def volume_object_path(resource_name: str, city_name: str, period: str | None):
    if period is None:
        return f"{city_name}/{resource_name}/{city_name}-{resource_name}.parquet"
    return f"{format_object_name(resource_name, city_name, period)}.parquet"


def _save_to_databricks(
    data_df: pd.DataFrame,
    resource_name: str,
    city_name: str,
    period: str | None,
    databricks_volume_resource: DatabricksVolumeResource,
):
    resource_path = volume_object_path(resource_name, city_name, period)
    return databricks_volume_resource.upload_dataframe(
        data_df, resource_path, overwrite=True
    )


def _get_unidades_gestoras_ids(
    city_name: str,
    databricks_volume_resource: DatabricksVolumeResource,
) -> list[str]:
    # unidades_gestoras has no time dimension: one parquet per city.
    resource_path = volume_object_path("unidades_gestoras", city_name, None)
    try:
        unidades_gestoras_df = databricks_volume_resource.download_dataframe(
            resource_path
        )
    except NotFound as exc:
        raise FileNotFoundError(
            f"unidades_gestoras not found at {resource_path}. Materialize "
            f"{df_asset_name('unidades_gestoras')} for partition {city_name} first."
        ) from exc
    return unidades_gestoras_df["id"].tolist()


def _fetch_per_unidade_gestora(
    api: str,
    config: PortalEndpoint,
    city_name: str,
    params: dict,
    databricks_volume_resource: DatabricksVolumeResource,
):
    ug_ids = _get_unidades_gestoras_ids(city_name, databricks_volume_resource)
    if not ug_ids:
        raise ValueError(f"No unidades gestoras found for {city_name}")

    ug_dfs = []
    for ug_id in ug_ids:
        fetched_data, url = _get_portal_data(
            api,
            city_name,
            config.endpoint,
            {**params, "unidade_gestora": ug_id},
            config.timeout,
        )
        ug_df = pd.DataFrame(fetched_data["resultado"][config.result_key])
        ug_df["unidade_gestora"] = ug_id
        ug_dfs.append(ug_df)

    return pd.concat(ug_dfs, ignore_index=True), url


def _build_portal_api_asset(api: str, group_name: str, config: PortalEndpoint):
    per_unidade_gestora = "unidade_gestora" in config.param_names
    partitions_def = partitions_def_for(config.param_names)
    # Every period of a city reads that city's (time-less) UG list.
    ug_dep = AssetDep(
        df_asset_name("unidades_gestoras"),
        partition_mapping=MultiToSingleDimensionPartitionMapping(
            partition_dimension_name="city"
        ),
    )

    @asset(
        name=df_asset_name(config.endpoint),
        deps=[ug_dep] if per_unidade_gestora else None,
        partitions_def=partitions_def,
        group_name=group_name,
        kinds=["databricks"],
    )
    def _portal_api_asset(
        context, databricks_volume_resource: DatabricksVolumeResource
    ):
        city_name, period = partition_scope(partitions_def, context.partition_key)
        params = _partition_params(config.param_names, period)

        if per_unidade_gestora:
            data_df, url = _fetch_per_unidade_gestora(
                api,
                config,
                city_name,
                params,
                databricks_volume_resource,
            )
        else:
            fetched_data, url = _get_portal_data(
                api, city_name, config.endpoint, params or None, config.timeout
            )
            data_df = pd.DataFrame(fetched_data["resultado"][config.result_key])

        data_df = serialize_nested_columns(data_df)

        volume_path = _save_to_databricks(
            data_df,
            config.endpoint,
            city_name,
            period,
            databricks_volume_resource,
        )

        return Output(
            data_df,
            metadata={
                "source": MetadataValue.url(url),
                "rows": len(data_df),
                "columns": list(data_df.columns),
                "databricks_volume_path": volume_path,
                "preview": MetadataValue.md(data_df.head().to_markdown()),
            },
        )

    return _portal_api_asset


def build_portal_api_assets(
    api: str, group_name: str, endpoints: list[PortalEndpoint]
):
    return [_build_portal_api_asset(api, group_name, config) for config in endpoints]
