import os

# gov_transparency_hub/__init__.py builds Definitions at import time and some
# resources resolve env vars eagerly; tests must not depend on a real .env.
_DUMMY_ENV = {
    "DATABRICKS_HOST": "https://test.cloud.databricks.com",
    "DATABRICKS_TOKEN": "test",
    "DATABRICKS_CATALOG": "inconfidente",
    "DATABRICKS_SCHEMA": "raw",
    "DATABRICKS_VOLUME": "raw",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
}

for key, value in _DUMMY_ENV.items():
    os.environ.setdefault(key, value)

import subprocess
import sys
from pathlib import Path

_DBT_DIR = Path(__file__).resolve().parent.parent / "gov_transparency_hub" / "dbt_pipelines"
_DBT = str(Path(sys.executable).parent / "dbt")

# @dbt_assets reads target/manifest.json at import time and target/ is
# gitignored. dbt parse renders profiles.yml (env vars above) but never
# connects to the warehouse.
if not (_DBT_DIR / "dbt_packages" / "dbt_utils").exists():
    subprocess.run([_DBT, "deps", "--quiet"], cwd=_DBT_DIR, check=True)
subprocess.run([_DBT, "parse", "--quiet"], cwd=_DBT_DIR, check=True)
