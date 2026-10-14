import json
import tempfile
from typing import Literal, NamedTuple

import dlt
import pandas as pd
from dagster import AssetsDefinition, MaterializeResult, MetadataValue, asset
from databricks.sdk.errors import NotFound
from dlt.common.normalizers.naming.snake_case import NamingConvention

from gov_transparency_hub.assets.portal_api_assets import (
    df_asset_name,
    serialize_nested_columns,
    volume_object_path,
)
from gov_transparency_hub.partitions import daily_city_partition
from gov_transparency_hub.resources import DatabricksVolumeResource
from gov_transparency_hub.resources.databricks_dlt_resource import (
    DatabricksDltResource,
)

RAW_DATASET = "raw"

# Same convention dlt applies at load time, so key_columns can be validated
# here with the exact names they will have in the destination table.
_NAMING = NamingConvention()


class RawTableConfig(NamedTuple):
    endpoint: str
    key_columns: tuple[str, ...]
    strategy: Literal["upsert", "delete-insert"] = "upsert"
    # Name as it appears in the parquet (before snake_case normalization).
    flatten_column: str | None = None
    drop_exact_duplicates: bool = False


def _parse_json_object(value, column: str, endpoint: str) -> dict:
    if value is None or value == "" or (
        isinstance(value, float) and pd.isna(value)
    ):
        return {}
    parsed = value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = value
    if not isinstance(parsed, dict):
        # Bad source data, not a caller bug: same ValueError as key validation.
        raise ValueError(  # noqa: TRY004
            f"{endpoint}: {column} must be a JSON object, "
            f"got {str(value)[:80]!r}"
        )
    return parsed


def _flatten_json_column(
    df: pd.DataFrame, column: str, endpoint: str
) -> pd.DataFrame:
    parsed = [
        _parse_json_object(value, column, endpoint) for value in df[column]
    ]
    flat = pd.json_normalize(parsed, max_level=0)
    flat.index = df.index
    return pd.concat([df.drop(columns=[column]), flat], axis=1)


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    normalized = [_NAMING.normalize_identifier(col) for col in df.columns]
    collisions = {col for col in normalized if normalized.count(col) > 1}
    if collisions:
        raise ValueError(
            f"Columns collide after snake_case normalization: {collisions}"
        )
    return df.set_axis(normalized, axis=1)


def _validate_keys(df: pd.DataFrame, config: RawTableConfig) -> None:
    key_columns = list(config.key_columns)
    missing = [col for col in key_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"{config.endpoint}: key columns missing from data: {missing}"
        )

    null_keys = df[key_columns].isna().any(axis=1)
    if null_keys.any():
        raise ValueError(
            f"{config.endpoint}: {int(null_keys.sum())} rows with null key "
            f"in {key_columns}"
        )
    if config.strategy != "upsert":
        return

    duplicated = df.duplicated(subset=key_columns, keep=False)
    if duplicated.any():
        sample = (
            df.loc[duplicated, key_columns]
            .drop_duplicates()
            .head(5)
            .to_dict("records")
        )
        raise ValueError(
            f"{config.endpoint}: duplicate key {key_columns}, e.g. {sample}"
        )


def prepare_raw_dataframe(
    df: pd.DataFrame,
    config: RawTableConfig,
    city_name: str,
    partition_year: str,
) -> pd.DataFrame:
    if config.flatten_column:
        df = _flatten_json_column(
            df, config.flatten_column, config.endpoint
        )
        df = serialize_nested_columns(df)
    df = _normalize_column_names(df)
    if config.drop_exact_duplicates:
        df = df.drop_duplicates(ignore_index=True)
    # Raw is all text: dlt fixes a column's type on the first load, so a
    # partition whose values all look like ints would truncate later floats
    # and turn ids like "01" into 1. Typing is dbt's job.
    df = df.astype("string")
    df = df.assign(municipio=city_name, ano_referencia=partition_year)
    _validate_keys(df, config)
    return df


def load_raw_table(
    df: pd.DataFrame,
    config: RawTableConfig,
    destination,
    pipelines_dir: str,
) -> str:
    key_columns = list(config.key_columns)
    # delete-insert must not get a primary_key: dlt would deduplicate the batch
    # and drop legitimate identical rows (e.g. two equal receita collections).
    key_hint = (
        {"primary_key": key_columns}
        if config.strategy == "upsert"
        else {"merge_key": key_columns}
    )
    resource = dlt.resource(
        df,
        name=config.endpoint,
        write_disposition={"disposition": "merge", "strategy": config.strategy},
        **key_hint,
    )
    pipeline = dlt.pipeline(
        pipeline_name=f"raw_{config.endpoint}",
        destination=destination,
        dataset_name=RAW_DATASET,
        pipelines_dir=pipelines_dir,
    )
    load_info = pipeline.run(resource)
    return load_info.loads_ids[0]


def _download_source(
    databricks_volume_resource: DatabricksVolumeResource,
    endpoint: str,
    city_name: str,
    partition_date_str: str,
) -> pd.DataFrame:
    path = volume_object_path(endpoint, city_name, partition_date_str)
    try:
        return databricks_volume_resource.download_dataframe(path)
    except NotFound as exc:
        raise FileNotFoundError(
            f"{endpoint} not found at {path}. Materialize "
            f"{df_asset_name(endpoint)} for partition "
            f"{partition_date_str}|{city_name} first."
        ) from exc


def _build_raw_load_asset(group_name: str, config: RawTableConfig):
    asset_name = f"raw_{config.endpoint}"

    @asset(
        name=asset_name,
        deps=[df_asset_name(config.endpoint)],
        partitions_def=daily_city_partition,
        group_name=group_name,
        kinds=["databricks", "dlt"],
        # One MERGE at a time per Delta table; concurrent MERGEs conflict.
        pool=asset_name,
    )
    def _raw_load_asset(
        context,
        databricks_volume_resource: DatabricksVolumeResource,
        databricks_dlt_resource: DatabricksDltResource,
    ):
        dimensions = context.partition_key.keys_by_dimension
        city_name = dimensions.get("city")
        partition_date_str = dimensions.get("date")
        table = (
            f"{databricks_dlt_resource.catalog}.{RAW_DATASET}."
            f"{config.endpoint}"
        )

        source_df = _download_source(
            databricks_volume_resource,
            config.endpoint,
            city_name,
            partition_date_str,
        )
        if source_df.empty:
            # Never run delete-insert with an empty batch: keep the last slice.
            return MaterializeResult(metadata={"rows": 0, "table": table})

        raw_df = prepare_raw_dataframe(
            source_df, config, city_name, partition_date_str[:4]
        )
        # Per-run dlt working dir: concurrent partitions must not share local
        # pipeline state; the canonical state lives in the destination.
        with tempfile.TemporaryDirectory() as pipelines_dir:
            load_id = load_raw_table(
                raw_df,
                config,
                databricks_dlt_resource.destination(),
                pipelines_dir,
            )

        return MaterializeResult(
            metadata={
                "rows": len(raw_df),
                "table": table,
                "load_id": load_id,
                "strategy": config.strategy,
                "preview": MetadataValue.md(raw_df.head().to_markdown()),
            }
        )

    return _raw_load_asset


def build_raw_load_assets(
    group_name: str, configs: list[RawTableConfig]
) -> list[AssetsDefinition]:
    return [_build_raw_load_asset(group_name, config) for config in configs]
