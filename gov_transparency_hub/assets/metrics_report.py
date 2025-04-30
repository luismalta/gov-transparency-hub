import pandas as pd
from datetime import date

from gov_transparency_hub.resources import S3Resource

from dagster import asset, Output, MetadataValue

def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "int"
    elif pd.api.types.is_float_dtype(dtype):
        return "float"
    elif pd.api.types.is_bool_dtype(dtype):
        return "bool"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "timestamp"
    else:
        return "string" 


@asset
def metrics_report(context, s3_resource: S3Resource):
    asset_keys = context.instance.get_asset_keys() # Pega todos os assets conhecidos
    report = {}

    for asset_key in asset_keys:
        asset_key_str = "/".join(asset_key.path)

        mat = context.instance.get_latest_materialization_event(asset_key)
        if mat:
            metadata = {k: v.value for k, v in mat.asset_materialization.metadata.items()}
        else:
            metadata = {}

        report[asset_key_str] = {
            "metadata": metadata,
        }
    report_df = pd.DataFrame.from_dict(report, orient="index")

    
    bucket_name = "metrics-report"
    object_name = f"{bucket_name}-{date.today().strftime('%Y-%m-%d')}"

    s3_resource.upload_object(bucket_name, object_name, report_df)

    return Output(
        report_df,
        metadata={
            "preview": MetadataValue.md(report_df.to_markdown()),
        },
    )
