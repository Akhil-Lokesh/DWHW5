from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import json

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id="Stock_Data_ETL",
    default_args=default_args,
    start_date=datetime(2025, 2, 21),
    catchup=False,
    schedule_interval="@daily",
    tags=["ETL", "Stock"],
) as dag:
    
    # Define constants
    STOCK_TABLE = "dev.raw_data.stock_data"
    STOCKS = ["NVDA", "AAPL"]
    
    @task
    def extract_stock_data():
        """
        Extract stock data from Alpha Vantage API
        """
        # Get Alpha Vantage API key from Airflow Variables
        api_key = Variable.get("alpha_vantage_api_key")
        all_stock_data = {}
        
        for stock in STOCKS:
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock}&outputsize=compact&apikey={api_key}"
            response = requests.get(url)
            data = response.json()
            
            if "Time Series (Daily)" in data:
                # Get the last 90 days of data
                time_series = data["Time Series (Daily)"]
                # Sort by date and get the first 90 entries
                sorted_dates = sorted(time_series.keys(), reverse=True)[:90]
                
                filtered_data = {date: time_series[date] for date in sorted_dates}
                all_stock_data[stock] = filtered_data
            else:
                raise ValueError(f"Failed to fetch data for {stock}: {data}")
        
        return all_stock_data
    
    @task
    def transform_stock_data(stock_data):
        """
        Transform the raw API data into a structured format
        """
        transformed_data = []
        
        for stock, time_series in stock_data.items():
            for date, values in time_series.items():
                transformed_data.append({
                    "stock_symbol": stock,
                    "date": date,
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"])
                })
        
        return transformed_data
    
    @task
    def create_table_if_not_exists():
        """
        Create the stock data table if it doesn't exist
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {STOCK_TABLE} (
            stock_symbol VARCHAR(10),
            date DATE,
            open FLOAT,
            close FLOAT,
            high FLOAT,
            low FLOAT,
            volume INT,
            PRIMARY KEY (stock_symbol, date)
        )
        """
        
        hook.run(create_table_sql)
        return "Table created or already exists"
    
    @task
    def load_to_snowflake(transformed_data):
        """
        Load transformed data to Snowflake using SQL transaction
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Prepare values for bulk insert
        values_list = []
        for row in transformed_data:
            value = f"('{row['stock_symbol']}', '{row['date']}', {row['open']}, {row['close']}, {row['high']}, {row['low']}, {row['volume']})"
            values_list.append(value)
        
        values_str = ",\n".join(values_list)
        
        # SQL for full refresh using transaction
        sql_statements = [
            f"BEGIN TRANSACTION;",
            f"DELETE FROM {STOCK_TABLE} WHERE stock_symbol IN ({', '.join([f\"'{stock}'\" for stock in STOCKS])});",
            f"INSERT INTO {STOCK_TABLE} (stock_symbol, date, open, close, high, low, volume) VALUES {values_str};",
            f"COMMIT;"
        ]
        
        try:
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            for statement in sql_statements:
                cursor.execute(statement)
                
            cursor.close()
            conn.close()
            return "Data loaded successfully"
        except Exception as e:
            # If an error occurs, rollback the transaction
            if 'cursor' in locals() and cursor:
                cursor.execute("ROLLBACK;")
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()
            raise e
    
    @task
    def verify_idempotency():
        """
        Verify the idempotency of the pipeline by checking record counts
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        record_count_sql = f"""
        SELECT COUNT(*) FROM {STOCK_TABLE} 
        WHERE stock_symbol IN ({', '.join([f"'{stock}'" for stock in STOCKS])})
        """
        
        result = hook.get_first(record_count_sql)
        return f"Total records: {result[0]}"
    
    # Define task dependencies
    table_task = create_table_if_not_exists()
    extracted_data = extract_stock_data()
    transformed_data = transform_stock_data(extracted_data)
    load_result = load_to_snowflake(transformed_data)
    verify_result = verify_idempotency()
    
    # Set dependencies
    table_task >> extracted_data >> transformed_data >> load_result >> verify_result
