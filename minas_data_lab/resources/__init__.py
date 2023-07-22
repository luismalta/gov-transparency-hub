import os
from .postgres_io_manager import PostgresIOManager
from .s3_io_manager import S3IOManager
from .postgres_resource import PostgresResource
from . import PortalTransparenciaScrapper

SHARED_POSTGRES_CONF = {
    "username": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
    "database": os.environ.get("POSTGRES_DATABASE", "minas_data_lab"),
}

SHARED_S3_CONF = {
    "endpoint": os.environ.get("S3_ENDPOINT", "localhost"),
    "port": os.environ.get("S3_PORT", "9000"),
    "access_key_id": os.environ.get("S3_ACCESS_KEY_ID", ""),
    "secret_access_key": os.environ.get("S3_SECRET_KEY", ""),
}

HOST = os.environ.get("POSTGRES_HOST", "localhost")
PORT = os.environ.get("POSTGRES_PORT", "15432")

RESOURCES_PROD = {
    "postgres_io_manager": PostgresIOManager(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "postgres_resource": PostgresResource(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "s3_io_manager": S3IOManager(**SHARED_S3_CONF),
}


RESOURCES_STAGING = {
    "postgres_io_manager": PostgresIOManager(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "postgres_resource": PostgresResource(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "s3_io_manager": S3IOManager(**SHARED_S3_CONF),
}


RESOURCES_LOCAL = {
    "postgres_io_manager": PostgresIOManager(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "postgres_resource": PostgresResource(host=HOST, port=PORT, **SHARED_POSTGRES_CONF),
    "s3_io_manager": S3IOManager(**SHARED_S3_CONF),
}