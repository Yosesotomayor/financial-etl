import os
from dagster import Definitions, load_assets_from_modules
from app.resources.db_conn import PostgresResource
from app import extract, transform, load
from dotenv import load_dotenv

load_dotenv()

etl_assets = load_assets_from_modules([extract, transform, load])

defs = Definitions(
    assets=etl_assets,
    resources={
        "db": PostgresResource(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST", "db"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DB"),
        )
    },
)
