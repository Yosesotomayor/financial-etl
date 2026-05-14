import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Portfolio Optimization Dashboard", layout="wide")

def get_db_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = "db"
    port = os.getenv("POSTGRES_PORT", 5432)
    database = "postgres"
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)

st.title("Portfolio Optimization Engine")
st.sidebar.header("Settings")

engine = get_db_engine()

try:
    # Cargar Pesos
    query_weights = "SELECT * FROM results.weights"
    df_weights = pd.read_sql(query_weights, engine)
    
    # Cargar Precios
    query_prices = "SELECT * FROM raw.daily_prices"
    df_prices = pd.read_sql(query_prices, engine, index_col='date')
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Maximum Sharpe Ratio Portfolio")
        df_sharpe = df_weights[df_weights['portfolio_type'] == 'max_sharpe']
        df_sharpe = df_sharpe[df_sharpe['weight'] > 0.01] # Solo pesos significativos
        fig_sharpe = px.pie(df_sharpe, values='weight', names='ticker', hole=0.4)
        st.plotly_chart(fig_sharpe, use_container_width=True)
        
    with col2:
        st.subheader("Minimum Variance Portfolio")
        df_var = df_weights[df_weights['portfolio_type'] == 'min_variance']
        df_var = df_var[df_var['weight'] > 0.01]
        fig_var = px.pie(df_var, values='weight', names='ticker', hole=0.4)
        st.plotly_chart(fig_var, use_container_width=True)
        
    st.divider()
    
    st.subheader("Historical Asset Performance (Normalized)")
    # Normalizar precios para comparar
    df_normalized = df_prices / df_prices.iloc[0]
    fig_prices = px.line(df_normalized, x=df_normalized.index, y=df_normalized.columns)
    st.plotly_chart(fig_prices, use_container_width=True)
    
    st.subheader("Detailed Allocation")
    tab1, tab2 = st.tabs(["Max Sharpe", "Min Variance"])
    
    with tab1:
        st.dataframe(df_sharpe.sort_values(by='weight', ascending=False).style.format({'weight': '{:.2%}'}), use_container_width=True)
        
    with tab2:
        st.dataframe(df_var.sort_values(by='weight', ascending=False).style.format({'weight': '{:.2%}'}), use_container_width=True)

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Ensure the ETL and Markowitz services have completed successfully.")

if st.sidebar.button("Refresh Data"):
    st.rerun()
