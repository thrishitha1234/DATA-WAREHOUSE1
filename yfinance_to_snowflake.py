from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime
import snowflake.connector
import yfinance as yf
from datetime import timedelta

# Function to get the next day's date
def get_next_day(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = date_obj + timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")

# Function to establish a Snowflake connection
def return_snowflake_conn():
    user_id = Variable.get('snowflake_username')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,
        warehouse='compute_wh',
        database='dev'
    )
    return conn.cursor()

# Function to get the logical date (execution date) from Airflow's context
def get_logical_date():
    from airflow.operators.python import get_current_context
    context = get_current_context()
    return str(context['logical_date'])[:10]

# Task to extract stock data using Yahoo Finance API
@task
def extract(symbol):
    date = get_logical_date()
    end_date = get_next_day(date)
    data = yf.download(symbol, start=date, end=end_date)
    return data.to_dict(orient="list")

# Task to load data into Snowflake
@task
def load(d, symbol, target_table):
    date = get_logical_date()
    cur = return_snowflake_conn()

    try:
        # Check if data exists before proceeding
        if len(d['Open']) == 0:
            raise ValueError("No data available to insert")

        # Ensure the table exists
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
            date date, open float, close float, high float, low float, volume int, symbol varchar 
        )""")

        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {target_table} WHERE date='{date}'")

        # Safely insert data using proper type formatting
        sql = f"""INSERT INTO {target_table} (date, open, close, high, low, volume, symbol) VALUES (
          '{date}', {float(d['Open'][0])}, {float(d['Close'][0])}, {float(d['High'][0])}, {float(d['Low'][0])}, {int(d['Volume'][0])}, '{symbol}')"""
        
        print(sql)
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error: {e}")
        raise e

# Define the DAG
with DAG(
    dag_id='YfinanceToSnowflake',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=['ETL'],
    schedule='30 2 * * *'  # Run daily at 2:30 AM
) as dag:
    
    target_table = "test.raw_data.stocks"
    symbol = "AAPL"

    # Task dependencies
    data = extract(symbol)
    load(data, symbol, target_table)
