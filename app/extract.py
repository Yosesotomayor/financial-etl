import pandas as pd
import yfinance as yf
from dagster import asset

@asset(compute_kind="yfinance", group_name="etl_flow")
def raw_market_data():
    """
    Extrae datos de yfinance. 
    Aquí es donde se pondrá la lógica real de descarga.
    """
    tickers = ["AAPL", "MSFT", "GOOGL"]
    # Simulacro para que no falle al probar la infra
    data = {"ticker": tickers, "price": [150, 400, 140]}
    return pd.DataFrame(data)
