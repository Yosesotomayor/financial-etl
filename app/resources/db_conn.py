from sqlalchemy import create_engine
from dagster import ConfigurableResource
from contextlib import contextmanager

class PostgresResource(ConfigurableResource):
    user: str
    password: str
    host: str
    port: int
    database: str

    def get_url(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @contextmanager
    def get_connection(self):
        engine = create_engine(self.get_url())
        with engine.connect() as conn:
            yield conn
        engine.dispose()
