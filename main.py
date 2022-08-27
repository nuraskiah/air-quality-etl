from datetime import datetime
import json
import os
from dotenv import load_dotenv
import requests
import pandas as pd
import hashlib
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

API_KEY = os.getenv('API_KEY')
BASE_URL = 'https://api.weatherbit.io/v2.0/history/airquality'
DATASET_ID = os.getenv('DATASET_ID')

def get_states():
    states = ['Jakarta Raya', 'Central Java', 'East Java', 'West Java', 'Yogyakarta']
    df_state = pd.read_csv('./data/states.csv')
    df_state = df_state.loc[df_state['state_name'].isin(states)]

    return df_state.to_dict('records')

def get_cities(states):
    state_codes = [state['state_code'] for state in states]

    df_city = pd.read_csv('./data/cities_20000.csv')
    df_city = df_city[df_city['country_full'] == 'Indonesia']
    df_city = df_city.loc[df_city['state_code'].isin(state_codes)]

    return df_city.to_dict('records')

def fetch(cities, start_date, end_date):
    for city in cities:
        res = requests.get(f'{BASE_URL}?city_id={city["city_id"]}&start_date={start_date}&end_date={end_date}&key={API_KEY}')
        res = res.json()
        res['city_id'] = city['city_id']
        yield res

def extract():
    states = get_states()
    cities = get_cities(states)
    aq_data = fetch(cities, '2022-08-24', '2022-08-25')

    return states, cities, aq_data

def transform(raw_data, key):
    for data in raw_data:
        yield {
            'super_key': hashlib.md5(str(data[key]).encode()).hexdigest(),
            'raw_data': json.dumps(data),
            'input_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

def load(table_id, data):
    project_id = credential.project_id

    credential = service_account.Credentials.from_service_account_file('credentials.json')
    client = bigquery.Client(
        credentials=credential,
        project=project_id,
    )
    
    schema = [
        bigquery.SchemaField("super_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("raw_data", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("input_time", "DATETIME", mode="NULLABLE"),
    ]
    
    table_ref = f'{project_id}.{DATASET_ID}.{table_id}'
    
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)

    client.insert_rows_json(table, data)
    
    print("Berhasil")

if __name__ == '__main__':
    states, cities, aq_data = extract()
    fix_state = transform(states, 'state_code')
    fix_city = transform(cities, 'city_id')
    fix_data = transform(aq_data)

    load('raw_states', fix_state)
    load('raw_cities', fix_city)
