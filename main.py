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

def get_cities():
    df_city = pd.read_csv('./cities_20000.csv')
    df_city = df_city[df_city['country_full'] == 'Indonesia']
    df_city = df_city.loc[df_city['state_code'].isin(['4','7','8','10','30'])]

    return df_city

def fetch(cities):
    for city in cities:
        res = requests.get(f'{BASE_URL}?city_id={city}&start_date=2022-08-19&end_date=2022-08-25&key={API_KEY}')
        yield res.json()

def extract():
    cities = get_cities()
    city_ids = cities['city_id'].values
    # aq_data = fetch(city_ids)

    transform(cities)

def transform():
    return


if __name__ == '__main__':
    print(API_KEY)
    print('hehe')
