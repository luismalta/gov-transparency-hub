from dagster import EnvVar
from .postgres_resource import PostgresResource
from .s3_resource import S3Resource
from .databricks_volume_resource import DatabricksVolumeResource
from .databricks_dlt_resource import DatabricksDltResource
from .dbt_connection import dbt_resource, dbt_project


SHARED_POSTGRES_CONF = {
    "username": EnvVar("POSTGRES_USER").get_value("postgres"),
    "password": EnvVar("POSTGRES_PASSWORD").get_value("postgres"),
    "database": EnvVar("POSTGRES_DATABASE").get_value("gov_transparency_hub"),
}

SHARED_S3_CONF = {
    "endpoint": EnvVar("S3_ENDPOINT").get_value("localhost"),
    "port": EnvVar("S3_PORT").get_value("9000"),
    "access_key_id": EnvVar("S3_ACCESS_KEY_ID").get_value(),
    "secret_access_key": EnvVar("S3_SECRET_KEY").get_value(),
}

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

HOST = EnvVar("POSTGRES_HOST").get_value("localhost")
PORT = EnvVar("POSTGRES_PORT").get_value("15432")
DATABRICKS_TOKEN = EnvVar("DATABRICKS_TOKEN").get_value(EnvVar("DATABRICKS_TOKEN_DEV").get_value())

RESOURCES_PROD = {
    "postgres_resource": PostgresResource(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "s3_resource": S3Resource(**SHARED_S3_CONF),
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
    "postgres_resource": PostgresResource(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "s3_resource": S3Resource(**SHARED_S3_CONF),
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
    "postgres_resource": PostgresResource(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "s3_resource": S3Resource(**SHARED_S3_CONF),
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
