import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from contextlib import contextmanager
from dagster import ConfigurableIOManager


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


class S3IOManager(ConfigurableIOManager):
    endpoint: str
    port: int
    access_key_id: str
    secret_access_key: str

    @property
    def _config(self):
        return self.dict()

    def handle_output(self, context, obj):
        bucket = context.asset_key.path[0]
        file_name = context.asset_key.path[1]

        with connect_s3(config=self._config) as s3_resource:
            date_str = datetime.now().strftime('%d-%m-%Y-')
            object_name = date_str + file_name

            parquet_buffer = BytesIO()
            obj.to_parquet(parquet_buffer)
            s3_resource.Object(bucket, object_name).put(Body=parquet_buffer.getvalue())
    
    def load_input(self, context):
        bucket = context.asset_key.path[0]
        file_name = context.asset_key.path[1]

        with connect_s3(config=self._config) as s3_resource:
            date_str = datetime.now().strftime('%d-%m-%Y-')
            object_name = date_str + file_name
            
            obj = s3_resource.Object(bucket, object_name)
            df = pd.read_parquet(BytesIO(obj.get()['Body'].read()))
            return df