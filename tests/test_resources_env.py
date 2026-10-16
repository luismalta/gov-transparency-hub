import os
import subprocess
import sys

# Resources resolve DATABRICKS_TOKEN at import time, so each scenario needs a
# fresh interpreter with its own environment.
_READ_TOKEN = (
    "from gov_transparency_hub.resources import RESOURCES_LOCAL; "
    "print(RESOURCES_LOCAL['databricks_dlt_resource'].token)"
)


def test_databricks_token_falls_back_to_dev_token():
    env = {k: v for k, v in os.environ.items() if k != "DATABRICKS_TOKEN"}
    env["DATABRICKS_TOKEN_DEV"] = "dev-token"

    result = subprocess.run(
        [sys.executable, "-c", _READ_TOKEN],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr[-500:]
    assert result.stdout.strip() == "dev-token"
