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
    
    @task
    def extract() -> Dict[str, Any]:
        """
        Extract stock data from Alpha Vantage API
        """
        # Get API key from Airflow variables
        api_key = Variable.get("alpha_vantage_api_key")
        stock_symbol = Variable.get("stock_symbol", default_var="AAPL")  # Default to AAPL if not set
        
        # API endpoint for daily time series
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&outputsize=compact&apikey={api_key}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()
            
            # Validate the response structure
            if "Time Series (Daily)" not in data:
                raise ValueError(f"Unexpected API response structure: {data.keys()}")
            
            return data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from Alpha Vantage: {e}")
            raise
        except ValueError as e:
            print(f"Error in API response: {e}")
            raise
    
    @task
    def transform(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform the raw API response into a structured format
        """
        try:
            # Extract time series data
            time_series = data["Time Series (Daily)"]
            
            # Transform into a list of records
            records = []
            symbol = Variable.get("stock_symbol", default_var="AAPL")
            
            # Get the last 90 days of data (API response is in descending order by date)
            count = 0
            for date, daily_data in time_series.items():
                if count >= 90:  # Limit to 90 days
                    break
                
                record = {
                    "stock_symbol": symbol,
                    "date": date,
                    "open": float(daily_data["1. open"]),
                    "high": float(daily_data["2. high"]),
                    "low": float(daily_data["3. low"]),
                    "close": float(daily_data["4. close"]),
                    "volume": int(daily_data["5. volume"])
                }
                records.append(record)
                count += 1
            
            return records
        except Exception as e:
            print(f"Error transforming data: {e}")
            raise
    
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
        symbol = Variable.get("stock_symbol", default_var="AAPL")
        
        # Prepare values for bulk insert
        values_list = []
        for row in transformed_data:
            value = f"('{row['stock_symbol']}', '{row['date']}', {row['open']}, {row['close']}, {row['high']}, {row['low']}, {row['volume']})"
            values_list.append(value)
        
        values_str = ",\n".join(values_list)
        
        # SQL for full refresh using transaction
        sql_statements = [
            f"BEGIN TRANSACTION;",
            f"DELETE FROM {STOCK_TABLE} WHERE stock_symbol = '{symbol}';",
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
        symbol = Variable.get("stock_symbol", default_var="AAPL")
        
        record_count_sql = f"""
        SELECT COUNT(*) FROM {STOCK_TABLE} 
        WHERE stock_symbol = '{symbol}'
        """
        
        result = hook.get_first(record_count_sql)
        return f"Total records for {symbol}: {result[0]}"
    
    # Define task dependencies
    table_task = create_table_if_not_exists()
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load_result = load_to_snowflake(transformed_data)
    verify_result = verify_idempotency()
    
    # Set dependencies
    table_task >> extracted_data >> transformed_data >> load_result >> verify_result
