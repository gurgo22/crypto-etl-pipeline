from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import pandas as pd
import pandas_gbq
from google.cloud import bigquery

COINGECKO_GET_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
DATA_FIELDS = ["id", "symbol", "name", "current_price", "market_cap", "price_change_percentage_24h", "last_updated"]
PROJECT_ID = 'airflow-warehouse-476721'
TABLE_ID = 'crypto_prices.crypto_prices_raw'

@task
def extract_crypto_api_data(**context):

    response = requests.get(COINGECKO_GET_URL)
    return response.json()

def is_data_valid(df):

    return not df.empty
    
@task
def transform_crypto_data(data_json):

    df = pd.DataFrame(data_json)

    df = df[DATA_FIELDS]

    if(not is_data_valid(df)):
        return None

    return df.to_json()

@task
def load_to_bigquery(df_json):

    df = pd.read_json(df_json)

    pandas_gbq.to_gbq(df, TABLE_ID, project_id=PROJECT_ID, if_exists="append", location="EU")

with DAG(
    dag_id='crypto_etl_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['crypto', 'api', 'bigquery'],
) as dag:

    extracted_data = extract_crypto_api_data() 

    transformed_data = transform_crypto_data(extracted_data)

    load_to_bigquery(transformed_data)