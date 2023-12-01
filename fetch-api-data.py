#  Build a function that can take in any country code and fetch the data and also carry out this transformation step.

import requests
import pandas as pd
import datetime as dt
import psycopg2
from sqlalchemy import create_engine


def fetch_data_from_api(country_code):
    baseurl = f'https://date.nager.at/api/v3/publicholidays/2023/{country_code}'
    try:
        response = requests.get(baseurl)
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")

# fetch data from API
data_from_api = fetch_data_from_api(country_code='NG')


if data_from_api:
    # tranform: create dataFrame with the fetched data
    holiday_df = pd.DataFrame(data_from_api)  

    # day (Monday, Tuesday, Friday etc.) each of those holiday falls in
    holiday_df['date'] = pd.to_datetime(holiday_df['date'])
    holiday_df['day_of_week'] = holiday_df['date'].dt.day_name()

    # load: connect to PostgreSQL dbms
    db_params = {
    'dbname': 'public_holiday',
    'user': 'postgres',
    'password': 'toludoyin',
    'host': 'localhost', 
    'port': '5432',      
}
    conn = psycopg2.connect(**db_params)
    conn.autocommit = True
    cursor  = conn.cursor()

    try:
        new_db_name = 'public_holiday'
        cursor.execute(f"select 1 from pg_catalog.pg_database where datname='{new_db_name}'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f'create database {new_db_name}')

            # connect to new db
            db_params['db_name']= new_db_name
            engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
        
            # write dataframe to db
            table_name = 'country_holiday'
            holiday_df.to_sql(table_name, engine,  if_exists = 'replace')
            print(f"Data successfully stored in {table_name} table in the {new_db_name} Database")
    
        else:
            print(f'Database {new_db_name} already exists')
    except psycopg2.Error as e:
        print(f'An error occured: {e}')
    finally:
        conn.close()

else:
    print("Data not available.")