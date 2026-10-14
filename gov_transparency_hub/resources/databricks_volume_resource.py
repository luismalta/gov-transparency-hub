from io import BytesIO

import pandas as pd
from dagster import ConfigurableResource
from databricks.sdk import WorkspaceClient


class DatabricksVolumeResource(ConfigurableResource):
    host: str
    token: str
    catalog: str
    # "schema" would shadow pydantic's BaseModel.schema
    schema_name: str
    volume: str

    def _get_client(self) -> WorkspaceClient:
        return WorkspaceClient(host=self.host, token=self.token)

    def _volume_path(self, file_path: str) -> str:
        return (
            f"/Volumes/{self.catalog}/{self.schema_name}/{self.volume}/"
            f"{file_path.lstrip('/')}"
        )

    def upload_dataframe(
        self, df: pd.DataFrame, file_path: str, overwrite: bool = True
    ) -> str:
        parquet_buffer = BytesIO()
        # Without index=False, Spark exposes a non-default pandas index as an
        # extra __index_level_0__ column.
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        volume_path = self._volume_path(file_path)
        self._get_client().files.upload(
            volume_path, parquet_buffer, overwrite=overwrite
        )
        return volume_path

    def download_dataframe(self, file_path: str) -> pd.DataFrame:
        volume_path = self._volume_path(file_path)
        response = self._get_client().files.download(volume_path)
        return pd.read_parquet(BytesIO(response.contents.read()))
