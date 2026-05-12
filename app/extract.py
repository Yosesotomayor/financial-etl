import yfinance as yf
from dagster import asset

@asset(compute_kind="yfinance", group_name="etl_flow")
def raw_market_data():

    tickers = ["AAPL", "MSFT", "T", # TECH
               "DPZ", "BIMBOA.MX", "SBUX", "KO", # CONSUMO
               "BABA","ITX.MC", "AMZN","LULU", # RETAIL
               "JNJ", "LLY", # HEALTHCARE
               "V", "CAT", "SHEL", "UMG.AS",] # VARIOS

    raw_data = yf.download(
        tickers,
        start = "2024-01-01", #Markovitz necesita mínimo 2 años de datos
        end = "2026-05-05",
        actions = True
    ) 
    if raw_data.empty:
        raise ValueError("No se pudieron descargar los datos de mercado.")

    return raw_data