from urllib.parse import urlparse

import dlt
from dagster import ConfigurableResource


class DatabricksDltResource(ConfigurableResource):
    host: str
    http_path: str
    token: str
    catalog: str

    def _server_hostname(self) -> str:
        # DATABRICKS_HOST is shared with the SDK, which expects a URL; the SQL
        # connector expects a bare hostname.
        return urlparse(self.host).hostname or self.host

    def destination(self):
        return dlt.destinations.databricks(
            credentials={
                "server_hostname": self._server_hostname(),
                "http_path": self.http_path,
                "access_token": self.token,
                "catalog": self.catalog,
            },
            # False would make dlt 1.31 also REMOVE the staging directory,
            # which the Files API rejects (404), failing every load job.
            # Staged parquet piles up in <dataset>_staging._dlt_staging_load_volume.
            keep_staged_files=True,
        )
