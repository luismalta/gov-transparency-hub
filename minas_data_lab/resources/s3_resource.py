import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from contextlib import contextmanager
from dagster import ConfigurableResource


@contextmanager
def connect_s3(config):

    try:
        s3_resource = boto3.resource(
            's3', 
            endpoint_url=f'http://{config["endpoint"]}:{config["port"]}',
            aws_access_key_id=config["access_key_id"],
            aws_secret_access_key=config["secret_access_key"],
            aws_session_token=None,
        )
        yield s3_resource
    finally:
        pass


class S3Resource(ConfigurableResource):
    endpoint: str
    port: int
    access_key_id: str
    secret_access_key: str

    @property
    def _config(self):
        return self.dict()

    def upload_object(self, bucket, filename, obj):

        with connect_s3(config=self._config) as s3_resource:
            parquet_buffer = BytesIO()
            obj.to_parquet(parquet_buffer)
            s3_resource.Object(bucket, filename).put(Body=parquet_buffer.getvalue())
    
    def get_object(self, bucket, filename):

        with connect_s3(config=self._config) as s3_resource:
            obj = s3_resource.Object(bucket, filename)
            df = pd.read_parquet(BytesIO(obj.get()['Body'].read()))
            return df