import os
from dotenv import load_dotenv
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

BASE_URL = 'https://api.weatherbit.io/v2.0/history/airquality'
API_KEY = os.getenv('API_KEY')

def get_states():
    states = ['Jakarta Raya', 'Central Java', 'East Java', 'West Java', 'Yogyakarta']
    df_state = pd.read_csv('./data/states.csv')
    df_state = df_state.loc[df_state['state_name'].isin(states)]

    return df_state.to_dict('records')

def get_cities(states):
    df_city = pd.read_csv('./data/cities_20000.csv')
    df_city = df_city[df_city['country_full'] == 'Indonesia']
    df_city = df_city.loc[df_city['state_code'].isin(states)]

    return df_city.to_dict('records')

def fetch(cities):
    for city in cities:
        res = requests.get(f'{BASE_URL}?city_id={city}&start_date=2022-08-19&end_date=2022-08-25&key={API_KEY}')
        yield res.json()

def extract():
    states = get_states()
    state_codes = [state['state_code'] for state in states]
    cities = get_cities(state_codes)
    city_ids = [city['city_id'] for city in cities]

    # aq_data = fetch(city_ids)

    # transform(cities)

def transform():
    return

def load():
    credential = service_account.Credentials.from_service_account_file('credentials.json')
    client = bigquery.Client(
        credentials=credential,
        project=credential.project_id,
    )

if __name__ == '__main__':
    extract()
