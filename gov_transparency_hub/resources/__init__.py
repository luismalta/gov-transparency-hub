from dagster import EnvVar
from .postgres_resource import PostgresResource
from .s3_resource import S3Resource
from .dbt_connection import dbt_resource


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

HOST = EnvVar("POSTGRES_HOST").get_value("localhost")
PORT = EnvVar("POSTGRES_PORT").get_value("15432")

RESOURCES_PROD = {
    "postgres_resource": PostgresResource(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "s3_resource": S3Resource(**SHARED_S3_CONF),
    "dbt": dbt_resource,
}


RESOURCES_STAGING = {
    "postgres_resource": PostgresResource(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "s3_resource": S3Resource(**SHARED_S3_CONF),
    "dbt": dbt_resource,
}


RESOURCES_LOCAL = {
    "postgres_resource": PostgresResource(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "s3_resource": S3Resource(**SHARED_S3_CONF),
    "dbt": dbt_resource,
}
