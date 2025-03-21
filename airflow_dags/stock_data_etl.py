from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

# Fetch Alpha Vantage API Key from Airflow Variables
ALPHA_VANTAGE_API_KEY = Variable.get("alpha_vantage_api_key")

# Function to return Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Define DAG
with DAG(
    dag_id="Stock_Data_ETL",
    start_date=datetime(2025, 2, 21),
    catchup=False,
    schedule_interval="@daily",
    tags=["ETL", "Stock"],
) as dag:

    # Define table names
    stock_table = "dev.raw_data.stock_data"
    STOCKS = ["NVDA", "AAPL"]
    cur = return_snowflake_conn()

    @task
    def fetch_stock_data():
        """
        Fetch stock price data from yfinance and load into Snowflake.
        """
        for stock in STOCKS:
            df = yf.download(stock, period="180d")
            df.reset_index(inplace=True)

            for _, row in df.iterrows():
                cur.execute(f"""
                    INSERT INTO {stock_table} (stock_symbol, date, open, close, low, high, volume)
                    VALUES ('{stock}', '{row['Date']}', {row['Open']}, {row['Close']}, {row['Low']}, {row['High']}, {row['Volume']});
                """)

    fetch_data_task = fetch_stock_data()
