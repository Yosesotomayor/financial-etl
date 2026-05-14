import os
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from scipy.optimize import minimize
from dotenv import load_dotenv

load_dotenv()

def get_db_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = "db"
    port = os.getenv("POSTGRES_PORT", 5432)
    database = "postgres"
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)

def load_data():
    engine = get_db_engine()
    print(f"Conectando a: {engine.url}", flush=True)
    # Esperar a que la tabla exista y tenga datos
    while True:
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT count(*) FROM raw.daily_prices"))
                count = result.scalar()
                print(f"Conteo de registros: {count}", flush=True)
                if count > 0:
                    break
            print("Esperando a que el ETL cargue datos en raw.daily_prices...", flush=True)
            time.sleep(10)
        except Exception as e:
            if "does not exist" in str(e):
                print("La tabla raw.daily_prices aún no existe...", flush=True)
            else:
                print(f"Error en el bucle de espera: {e}", flush=True)
            time.sleep(10)

    query = "SELECT * FROM raw.daily_prices"
    df = pd.read_sql(query, engine, index_col='date')
    return df

def markowitz_optimization(df):
    # Calcular retornos logarítmicos
    returns = np.log(df / df.shift(1)).dropna()
    
    mean_returns = returns.mean() * 252
    cov_matrix = returns.cov() * 252
    
    num_assets = len(mean_returns)
    
    # Función para minimizar (varianza del portafolio)
    def portfolio_variance(weights):
        return np.dot(weights.T, np.dot(cov_matrix, weights))

    # Función para maximizar Sharpe Ratio (minimizamos -Sharpe)
    def negative_sharpe(weights, risk_free_rate=0.02):
        p_return = np.sum(mean_returns * weights)
        p_volatility = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
        return -(p_return - risk_free_rate) / p_volatility

    constraints = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
    bounds = tuple((0, 1) for _ in range(num_assets))
    init_guess = num_assets * [1. / num_assets]
    
    # Optimización para Máximo Sharpe Ratio
    optimized_sharpe = minimize(negative_sharpe, init_guess, method='SLSQP', bounds=bounds, constraints=constraints)
    
    # Optimización para Mínima Varianza
    optimized_variance = minimize(portfolio_variance, init_guess, method='SLSQP', bounds=bounds, constraints=constraints)
    
    return optimized_sharpe.x, optimized_variance.x

def save_results(tickers, weights_sharpe, weights_var):
    engine = get_db_engine()

    # Crear DataFrames para los resultados
    df_sharpe = pd.DataFrame({
        'ticker': tickers,
        'weight': weights_sharpe,
        'portfolio_type': 'max_sharpe'
    })

    df_var = pd.DataFrame({
        'ticker': tickers,
        'weight': weights_var,
        'portfolio_type': 'min_variance'
    })

    results_df = pd.concat([df_sharpe, df_var])

    with engine.connect() as conn:
        print("Asegurando que el esquema results exista...", flush=True)
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS results"))
        conn.commit()
        
        print("Guardando resultados en results.weights...", flush=True)
        results_df.to_sql(
            name="weights",
            schema="results",
            con=conn,
            if_exists="replace",
            index=False
        )
        print("Resultados guardados exitosamente.", flush=True)

if __name__ == "__main__":
    print("Iniciando Servicio de Optimización de Markowitz...", flush=True)
    try:
        df = load_data()
        if df.empty:
            print("No se encontraron datos en la base de datos.", flush=True)
        else:
            tickers = df.columns
            weights_sharpe, weights_var = markowitz_optimization(df)

            # Guardar en DB
            save_results(tickers, weights_sharpe, weights_var)

            print("\n" + "="*40, flush=True)

            print("RESULTADOS DE OPTIMIZACIÓN DE MARKOWITZ")
            print("="*40)
            
            print("\n--- Portafolio de Máximo Sharpe Ratio ---")
            for ticker, weight in zip(tickers, weights_sharpe):
                if weight > 0.01:
                    print(f"{ticker}: {weight:.2%}")
                    
            print("\n--- Portafolio de Mínima Varianza ---")
            for ticker, weight in zip(tickers, weights_var):
                if weight > 0.01:
                    print(f"{ticker}: {weight:.2%}")
            print("="*40)
            
            print("\nOptimización completada. El servicio permanecerá activo.")
            while True:
                time.sleep(3600)
                
    except Exception as e:
        print(f"Error crítico: {e}")
