import boto3
import pandas as pd
from io import BytesIO
from contextlib import contextmanager
from dagster import ConfigurableResource


@contextmanager
def connect_s3(config):
    try:
        s3_resource = boto3.resource(
            "s3",
            endpoint_url=f"http://{config['endpoint']}:{config['port']}",
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

    def upload_object(self, bucket_name, filename, obj):
        with connect_s3(config=self._config) as s3_resource:
            bucket = s3_resource.Bucket(bucket_name)
            if not bucket.creation_date:
                s3_resource.create_bucket(Bucket=bucket_name)

            parquet_buffer = BytesIO()
            obj.to_parquet(parquet_buffer)
            s3_resource.Object(bucket_name, filename).put(
                Body=parquet_buffer.getvalue()
            )

    def get_object(self, bucket_name, filename):
        with connect_s3(config=self._config) as s3_resource:
            bucket = s3_resource.Bucket(bucket_name)
            if not bucket.creation_date:
                s3_resource.create_bucket(Bucket=bucket_name)

            obj = s3_resource.Object(bucket_name, filename)
            df = pd.read_parquet(BytesIO(obj.get()["Body"].read()))
            return df

    def upload_html(self, bucket_name, filename, html):
        with connect_s3(config=self._config) as s3_resource:
            if not s3_resource.Bucket(bucket_name).creation_date:
                s3_resource.create_bucket(Bucket=bucket_name)

            s3_resource.Object(bucket_name, filename).put(Body=html)

    def download_html(self, bucket_name, filename):
        with connect_s3(config=self._config) as s3_resource:
            obj = s3_resource.Object(bucket_name, filename)
            html_content = obj.get()["Body"].read().decode("utf-8")
            return html_content

    def download_folder_contents(self, bucket_name, folder_name):
        with connect_s3(config=self._config) as s3_resource:
            bucket = s3_resource.Bucket(bucket_name)
            objects = bucket.objects.filter(Prefix=folder_name)
            return [obj.get()["Body"].read() for obj in objects]
