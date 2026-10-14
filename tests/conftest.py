import os

# gov_transparency_hub/__init__.py builds Definitions at import time and some
# resources resolve env vars eagerly; tests must not depend on a real .env.
_DUMMY_ENV = {
    "S3_ACCESS_KEY_ID": "test",
    "S3_SECRET_KEY": "test",
    "DATABRICKS_HOST": "https://test.cloud.databricks.com",
    "DATABRICKS_TOKEN": "test",
    "DATABRICKS_CATALOG": "inconfidente",
    "DATABRICKS_SCHEMA": "raw",
    "DATABRICKS_VOLUME": "raw",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
}

for key, value in _DUMMY_ENV.items():
    os.environ.setdefault(key, value)
