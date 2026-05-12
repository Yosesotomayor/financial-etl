import pandas as pd
import pandas_ta as ta
import numpy as np
from dagster import asset

@asset(compute_kind="pandas", group_name="etl_flow")
# Limpieza y estructura de los datos crudos
def processed_market_data(raw_market_data):

    # .xs (cross-section) que es la forma más robusta de 
    # sacar una sección de un MultiIndex de Pandas
    try:
        # Extraer todos los cierres ajustados de todos los tickers
        adj_close = raw_market_data.xs('Adj Close', axis=1, level=0)
        # Extraer todos los volúmenes de todos los tickers
        volume = raw_market_data.xs('Volume', axis=1, level=0)
    except KeyError:
        print(f"Columnas disponibles: {raw_market_data.columns}")
        # Intento de emergencia por si se llaman distinto
        adj_close = raw_market_data['Close'] 
        volume = raw_market_data['Volume']

    # Limpieza
    adj_close = adj_close.ffill().dropna()
    volume = volume.loc[adj_close.index]

    return {"prices": adj_close, "volume": volume}

@asset(compute_kind="pandas", group_name="etl_flow")
# Calculo de retornos y matrices de covarianza para Markovitz
def markowitz_input(processed_market_data):
    # Extraemos 'prices' del diccionario que devuelve el asset anterior
    prices = processed_market_data["prices"]

    # Log returns para modelos estadísticos
    log_returns = np.log(prices / prices.shift(1)).dropna()

    # Retorno promedio esperado (anualizado 252 días)
    expected_returns = log_returns.mean() * 252

    # Matriz de covarianza anualizada
    cov_matrix = log_returns.cov() * 252

    return {
        "returns": log_returns,
        "avg_returns": expected_returns,
        "cov_matrix": cov_matrix
    }

@asset(compute_kind="pandas", group_name="etl_flow")
# Genera indicadores técnicos y variables objetivo para ML.
def ml_features(processed_market_data):

    prices = processed_market_data["prices"]
    volume = processed_market_data["volume"]
    tickers = prices.columns

    all_features = {}

    for ticker in tickers:
        df = pd.DataFrame(index=prices.index)
        df['price'] = prices[ticker]
        df['volume'] = volume[ticker]

        # Indicadores técnicos
        # Media móvil simple (tendencia)
        df['sma_20'] = ta.sma(df['price'], length = 20)
        # RSI (Fuerza relativa/sobrecompra-sobreventa)
        df['rsi'] = ta.rsi(df['price'], length = 14)
        # Volatilidad (desviación estándar 20 días)
        df['volatility'] = df['price'].rolling(window = 20).std()

        # Target (el precio subirá? 0 o 1)
        df['target'] = (df['price'].shift(-1) > df['price']).astype(int)

        all_features[ticker] = df.dropna()

    return all_features