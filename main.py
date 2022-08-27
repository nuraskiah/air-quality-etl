import os
from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

BASE_URL = 'https://api.weatherbit.io/v2.0/history/airquality'
API_KEY = os.getenv('API_KEY')

def get_states():
    states = ['Jakarta Raya', 'Central Java', 'East Java', 'West Java', 'Yogyakarta']
    df_state = pd.read_csv('./states.csv')
    df_state = df_state.loc[df_state['state_name'].isin(states)]

    return df_state.to_dict('records')

def get_cities(states):
    df_city = pd.read_csv('./cities_20000.csv')
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


if __name__ == '__main__':
    extract()
