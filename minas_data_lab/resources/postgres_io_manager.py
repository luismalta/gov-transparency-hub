import pandas as pd
from contextlib import contextmanager
from dagster import ConfigurableIOManager
from sqlalchemy import create_engine, URL


@contextmanager
def connect_postgres(config, schema="public"):
    url = URL.create(
        "postgresql+psycopg2",
        username=config["username"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        database=config["database"],
    )

    conn = None
    try:
        conn = create_engine(url).connect()
        yield conn
    finally:
        if conn:
            conn.close()


class PostgresIOManager(ConfigurableIOManager):
    username: str
    password: str
    host: str
    port: int
    database: str

    @property
    def _config(self):
        return self.dict()

    def handle_output(self, context, obj):
        table = context.asset_key.path[0]

        if isinstance(obj, pd.DataFrame):
            with connect_postgres(config=self._config) as engine:
                obj.to_sql(table, engine, if_exists='append', index=False)
        else:
            raise Exception(
                "PostgresIOManager only supports pandas DataFrames"
            )

    def load_input(self, context):
        raise NotImplementedError
