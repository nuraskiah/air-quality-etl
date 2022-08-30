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

def get_breakpoints():
    df_breakpoints = pd.read_csv('./data/aqi_breakpoints.csv')
    df_breakpoints.rename(columns={
        'Parameter Name': 'parameter_name',
        'Parameter Formula': 'parameter_formula',
        'Duration': 'duration',
        'Category': 'category',
        'Low Breakpoint': 'low',
        'High Breakpoint': 'high',
        'Unit': 'unit',
    }, inplace=True)

    ids = list(map(lambda x: f'{x[1]}-{str(x[0]+1)}' , enumerate(df_breakpoints.parameter_formula.values)))
    df_breakpoints.insert(0, 'id', ids, True)

    return df_breakpoints.to_dict('records')

def fetch(cities, start_date, end_date):
    for city in cities:
        endpoint = f'{BASE_URL}?city_id={city["city_id"]}&start_date={start_date}&end_date={end_date}&key={API_KEY}'
        response = requests.get(endpoint)
        response = response.json()
        response['city_id'] = city['city_id']
        yield response

def extract():
    states = get_states()
    cities = get_cities(states)
    aq_data = fetch(cities, '2022-08-21', '2022-08-28')
    breakpoints = get_breakpoints()

    return states, cities, aq_data, breakpoints

def transform(raw_data, key):
    for data in raw_data:
        yield {
            'super_key': hashlib.md5(str(data[key]).encode()).hexdigest(),
            'raw_data': json.dumps(data),
            'input_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

def load(table_id, data):
    credential = service_account.Credentials.from_service_account_file('credentials.json')
    project_id = credential.project_id
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

    errors = client.insert_rows_json(table, data)
    if errors == []:
        print(f'Data loaded to {table_id}. {str(len(list(data)))} rows have been added.')
    else:
        print('Encountered errors while inserting rows: {}'.format(errors))

if __name__ == '__main__':
    states, cities, aq_data, breakpoints = extract()

    fix_state = transform(states, 'state_code')
    fix_city = transform(cities, 'city_id')
    fix_data = transform(aq_data, 'city_id')
    fix_breakpoints = transform(breakpoints, 'id')

    load('raw_states', fix_state)
    load('raw_cities', fix_city)
    load('raw_air_qualities', fix_data)
    load('raw_breakpoints', fix_breakpoints)
