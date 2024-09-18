import re
import os
import psycopg2
import pandas as pd
import json
from urllib.parse import urlparse
from dotenv import load_dotenv

# Function to load and normalize the weather_data.json file
def load_weather_data(json_file_path):
    with open(json_file_path, 'r') as file:
        weather_data = json.load(file)
    
    # Normalize the 'data' field from the JSON, while keeping relevant fields
    weather_df = pd.json_normalize(weather_data, 'data', ['lat', 'lon', 'city_name', 'station_id', 'timezone'])
    
    return weather_df

def fetch_data_from_db():

    load_dotenv()

    # Fetch the database URL from the .env file
    PRODUCTION_URL = os.getenv("PRODUCTION_KEY")

    # Parse the URL to extract connection parameters
    url = urlparse(PRODUCTION_URL)

    conn_params = {
        'dbname': url.path[1:],    # Extracts the database name after '/'
        'user': url.username,       # Extracts the username
        'password': url.password,   # Extracts the password
        'host': url.hostname,       # Extracts the host
        'port': url.port            # Extracts the port
    }

    # Establish a connection to the database
    conn = psycopg2.connect(**conn_params)
    
    # Create a query to fetch all data from raw_messages
    query = "SELECT * FROM raw_messages_cleaned;"
    
    # Fetch the data into a pandas DataFrame
    raw_messages_df = pd.read_sql(query, conn)
    
    # Close the connection
    conn.close()
    
    return raw_messages_df

def main():

    try:
        # Fetch the data from raw_messages_cleaned table (instead of raw_messages)
        raw_messages_cleaned_df = fetch_data_from_db()

        # Load weather data from the JSON file
        weather_json_path = '/workspaces/Xomnia-Assignment/weather_data.json'  # Replace with the actual path to your JSON file
        weather_df = load_weather_data(weather_json_path)

        # Convert datetime fields in both DataFrames using the specified format
        weather_df['datetime'] = weather_df['datetime'].str.replace(':', ' ', regex=False) + ':00'

        # Now convert to datetime format
        weather_df['datetime'] = pd.to_datetime(weather_df['datetime'], format='%Y-%m-%d %H:%M')

        # Round datetime columns to the same granularity (if needed)
        weather_df['datetime'] = weather_df['datetime'].dt.round('min')
        raw_messages_cleaned_df['datetime'] = raw_messages_cleaned_df['datetime'].dt.round('min')


        print(weather_df.dtypes, '\n', raw_messages_cleaned_df.dtypes)

        # Merge raw_messages_cleaned_df with weather_df on 'datetime', 'latitude', and 'longitude'
        combined_df = pd.merge(
            raw_messages_cleaned_df, 
            weather_df, 
            how='left', 
            left_on=['datetime', 'latitude', 'longitude'], 
            right_on=['datetime', 'lat', 'lon']
        )

        # Display or save the combined DataFrame
        print(combined_df.head())  # Display the first few rows of the combined DataFrame

    finally:
        None


# Entry point of the script
if __name__ == "__main__":
    main()


