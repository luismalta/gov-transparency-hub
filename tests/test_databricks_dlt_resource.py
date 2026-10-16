import pytest

from gov_transparency_hub.resources import RESOURCES_LOCAL
from gov_transparency_hub.resources.databricks_dlt_resource import (
    DatabricksDltResource,
)


def _resource(host):
    return DatabricksDltResource(
        host=host,
        http_path="/sql/1.0/warehouses/abc",
        token="secret",
        catalog="inconfidente",
    )


@pytest.mark.parametrize(
    "host",
    ["https://dbc-123.cloud.databricks.com", "dbc-123.cloud.databricks.com"],
)
def test_destination_uses_bare_hostname(host):
    destination = _resource(host).destination()
    credentials = destination.config_params["credentials"]

    assert credentials["server_hostname"] == "dbc-123.cloud.databricks.com"
    assert credentials["http_path"] == "/sql/1.0/warehouses/abc"
    assert credentials["catalog"] == "inconfidente"
    assert credentials["access_token"] == "secret"


def test_destination_keeps_staged_files():
    # dlt 1.31's cleanup also REMOVEs the staging directory, which the
    # Databricks Files API rejects with 404 and fails every load job.
    destination = _resource("dbc-123.cloud.databricks.com").destination()

    assert destination.config_params["keep_staged_files"] is True


def test_resource_is_registered():
    assert "databricks_dlt_resource" in RESOURCES_LOCAL
