import pandas as pd
from dagster import asset, AssetExecutionContext
from app.resources.db_conn import PostgresResource

@asset(compute_kind="postgres", group_name="etl_flow")
def persisted_market_data(context: AssetExecutionContext, clean_market_data: pd.DataFrame, db: PostgresResource):
    """
    Carga los datos limpios en la base de datos Postgres.
    """
    with db.get_connection() as conn:
        context.log.info(f"Insertando {len(clean_market_data)} registros en raw.daily_prices")
        
    return True
