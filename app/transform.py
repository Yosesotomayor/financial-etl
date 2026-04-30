import pandas as pd
from dagster import asset

@asset(compute_kind="pandas", group_name="etl_flow")
def clean_market_data(raw_market_data: pd.DataFrame):
    """
    Toma los datos crudos y los limpia.
    """
    df = raw_market_data.copy()
    # Ejemplo: Asegurar que los precios sean flotantes y no haya nulos
    df["price"] = df["price"].astype(float)
    return df
