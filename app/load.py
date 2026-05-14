import pandas as pd
from dagster import asset, AssetExecutionContext
from app.resources.db_conn import PostgresResource

@asset(compute_kind="postgres", group_name="etl_flow")
def persisted_market_data(
    context: AssetExecutionContext, 
    processed_market_data: dict, # Recibe el output del asset de transformación
    db: PostgresResource
):
    # Carga de los datos limpios en la base de datos Postgres.

    # Extraer el df de precios del diccionario del transform
    clean_prices = processed_market_data["prices"]
    
    with db.get_connection() as conn:
        context.log.info(f"Insertando {len(clean_prices)} registros en raw.daily_prices")

        try:
            from sqlalchemy import text
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            conn.commit()
            
            # guardar en db (conn compatible?)
            clean_prices.to_sql(
                name="daily_prices",
                schema="raw",
                con=conn,
                if_exists="replace", # Usamos replace para asegurar que el esquema coincida con el DF
                index=True,          # True para que se guarde la columna de fechas (el indice)
                index_label="date"   # Nombre a la columna de fechas
            )
            context.log.info("Carga en raw.daily_prices completada exitosamente.")
        except Exception as e:
            context.log.error(f"Error al insertar en DB: {str(e)}")
            raise e

    return True