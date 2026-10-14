import json
from typing import NamedTuple

import pandas as pd
from dagster import MetadataValue, Output, asset
from databricks.sdk.errors import NotFound

from gov_transparency_hub.assets.utils import format_object_name
from gov_transparency_hub.partitions import daily_city_partition
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


def _partition_params(param_names: tuple[str, ...], partition_date_str: str) -> dict:
    year, month = partition_date_str[:4], partition_date_str[5:7]
    values = {
        "exercicio": year,
        "data_do_empenho_inicial": partition_date_str,
        "data_do_empenho_final": partition_date_str,
        "exercicio_de_criacao": year,
        "mes_de_criacao": month,
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


def volume_object_path(resource_name: str, city_name: str, partition_date_str: str):
    return f"{format_object_name(resource_name, city_name, partition_date_str)}.parquet"


def _save_to_databricks(
    data_df: pd.DataFrame,
    resource_name: str,
    city_name: str,
    partition_date_str: str,
    databricks_volume_resource: DatabricksVolumeResource,
):
    resource_path = volume_object_path(resource_name, city_name, partition_date_str)
    return databricks_volume_resource.upload_dataframe(
        data_df, resource_path, overwrite=True
    )


def _get_unidades_gestoras_ids(
    city_name: str,
    partition_date_str: str,
    databricks_volume_resource: DatabricksVolumeResource,
) -> list[str]:
    resource_path = volume_object_path(
        "unidades_gestoras", city_name, partition_date_str
    )
    try:
        unidades_gestoras_df = databricks_volume_resource.download_dataframe(
            resource_path
        )
    except NotFound as exc:
        raise FileNotFoundError(
            f"unidades_gestoras not found at {resource_path}. Materialize "
            f"unidades_gestoras for partition {partition_date_str}|{city_name} first."
        ) from exc
    return unidades_gestoras_df["id"].tolist()


def _fetch_per_unidade_gestora(
    api: str,
    config: PortalEndpoint,
    city_name: str,
    params: dict,
    partition_date_str: str,
    databricks_volume_resource: DatabricksVolumeResource,
):
    ug_ids = _get_unidades_gestoras_ids(
        city_name, partition_date_str, databricks_volume_resource
    )
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

    @asset(
        name=df_asset_name(config.endpoint),
        deps=[df_asset_name("unidades_gestoras")] if per_unidade_gestora else None,
        partitions_def=daily_city_partition,
        group_name=group_name,
        kinds=["databricks"],
    )
    def _portal_api_asset(
        context, databricks_volume_resource: DatabricksVolumeResource
    ):
        dimensions = context.partition_key.keys_by_dimension
        city_name = dimensions.get("city")
        partition_date_str = dimensions.get("date")
        params = _partition_params(config.param_names, partition_date_str)

        if per_unidade_gestora:
            data_df, url = _fetch_per_unidade_gestora(
                api,
                config,
                city_name,
                params,
                partition_date_str,
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
            partition_date_str,
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
