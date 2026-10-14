from dagster import EnvVar

from .databricks_dlt_resource import DatabricksDltResource
from .databricks_volume_resource import DatabricksVolumeResource
from .dbt_connection import dbt_resource

# EnvVar is passed unresolved so it is only read at runtime, avoiding
# validation errors at import time when Databricks is not configured.
SHARED_DATABRICKS_VOLUME_CONF = {
    "host": EnvVar("DATABRICKS_HOST"),
    "catalog": EnvVar("DATABRICKS_CATALOG"),
    "schema_name": EnvVar("DATABRICKS_SCHEMA"),
    "volume": EnvVar("DATABRICKS_VOLUME"),
}

SHARED_DATABRICKS_DLT_CONF = {
    "host": EnvVar("DATABRICKS_HOST"),
    "http_path": EnvVar("DATABRICKS_HTTP_PATH"),
    "catalog": EnvVar("DATABRICKS_CATALOG"),
}

DATABRICKS_TOKEN = EnvVar("DATABRICKS_TOKEN").get_value(EnvVar("DATABRICKS_TOKEN_DEV").get_value())

RESOURCES_PROD = {
    "databricks_volume_resource": DatabricksVolumeResource(
        token=DATABRICKS_TOKEN,
        **SHARED_DATABRICKS_VOLUME_CONF
    ),
    "databricks_dlt_resource": DatabricksDltResource(
        token=DATABRICKS_TOKEN,
        **SHARED_DATABRICKS_DLT_CONF
    ),
    "dbt": dbt_resource,
}


RESOURCES_STAGING = {
    "databricks_volume_resource": DatabricksVolumeResource(
        token=DATABRICKS_TOKEN,
        **SHARED_DATABRICKS_VOLUME_CONF
    ),
    "databricks_dlt_resource": DatabricksDltResource(
        token=DATABRICKS_TOKEN,
        **SHARED_DATABRICKS_DLT_CONF
    ),
    "dbt": dbt_resource,
}


RESOURCES_LOCAL = {
    "databricks_volume_resource": DatabricksVolumeResource(
        token=DATABRICKS_TOKEN,
        **SHARED_DATABRICKS_VOLUME_CONF
    ),
    "databricks_dlt_resource": DatabricksDltResource(
        token=DATABRICKS_TOKEN,
        **SHARED_DATABRICKS_DLT_CONF
    ),
    "dbt": dbt_resource,
}
