import re
import os
import psycopg2
import pandas as pd
import json
from urllib.parse import urlparse
from dotenv import load_dotenv
from raw_data_db_insert import create_cursor_and_insert_data, copy_from_csv
from exploratory_data_analysis import fetch_data_from_db, robust_clean_raw_message

# Function to save the DataFrame to a CSV file temporarily
def save_df_to_csv(df, file_path):
    df.to_csv(file_path, index=False)  # Save DataFrame to CSV

# Function to load and normalize the weather_data.json file
def load_weather_data(json_file_path):
    with open(json_file_path, 'r') as file:
        weather_data = json.load(file)
    
    # Normalize the 'data' field from the JSON, while keeping relevant fields
    weather_df = pd.json_normalize(weather_data, 'data', ['lat', 'lon', 'city_name', 'station_id', 'timezone'])
    
    return weather_df

def filter_weather_data(weather_df):
    # Convert datetime fields in both DataFrames to ensure they are in the same format
    # Clean the 'datetime' column by replacing ":" with a space and appending ":00" where needed
    weather_df['datetime'] = weather_df['datetime'].str.replace(':', ' ', regex=False) + ':00'
    weather_df['datetime'] = pd.to_datetime(weather_df['datetime'], errors='coerce')

    # Filter weather_df
    weather_df['datetime'] = weather_df['datetime'].dt.round('H')
    weather_df['lat'] = weather_df['lat'].astype(float).round(2)
    weather_df['lon'] = weather_df['lon'].astype(float).round(2)

    return weather_df

def filter_raw_messages_clean_df(raw_messages_df):
    # Convert Unix timestamps to a readable datetime format
    raw_messages_df['datetime'] = pd.to_datetime(raw_messages_df['datetime'], unit='s')
    
    # Apply the robust cleaning function
    raw_messages_df['cleaned_message'] = raw_messages_df['raw_message'].apply(robust_clean_raw_message)

    # Normalize the cleaned data into separate columns
    cleaned_columns_df = pd.json_normalize(raw_messages_df['cleaned_message'])

    # Concatenate the cleaned columns back to the original dataframe
    raw_messages_clean_df = pd.concat([raw_messages_df, cleaned_columns_df], axis=1)

    # Remove the 'raw_message' and 'cleaned_message' columns
    raw_messages_clean_df = raw_messages_clean_df.drop(columns=['raw_message', 'cleaned_message'])

    # Filter raw_messages_clean_df
    raw_messages_clean_df = raw_messages_clean_df.rename(columns={"latitude": "lat", "longitude": "lon"})
    raw_messages_clean_df['datetime'] = pd.to_datetime(raw_messages_clean_df['datetime'], errors='coerce')
    raw_messages_clean_df['datetime'] = raw_messages_clean_df['datetime'].dt.round('H')
    raw_messages_clean_df['lat'] = raw_messages_clean_df['lat'].astype(float).round(2)
    raw_messages_clean_df['lon'] = raw_messages_clean_df['lon'].astype(float).round(2)

    return raw_messages_clean_df



def main():

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

    try:
        # Fetch the data from raw_messages table
        raw_messages_df = fetch_data_from_db(query="SELECT * FROM raw_messages;", environment="STAGING")
        raw_messages_clean_df = filter_raw_messages_clean_df(raw_messages_df)

        # # Save the combined DataFrame to a temporary CSV file
        # csv_file_path = '/tmp/raw_messages_cleaned.csv'
        # save_df_to_csv(raw_messages_clean_df, csv_file_path)

        # # Insert the combined data into the raw_messages_cleaned table
        # create_cursor_and_insert_data(conn, csv_file_path, 'raw_messages_cleaned')

        # Load weather data from the JSON file
        weather_json_path = '/workspaces/Xomnia-Assignment/data/weather_data.json'  # Replace with actual path to your JSON file
        weather_df = load_weather_data(weather_json_path)
        weather_df = filter_weather_data(weather_df)

        # Combine the two dataframes
        combined_df = pd.merge(raw_messages_clean_df, weather_df, how='left', on=['datetime', 'lat', 'lon'])


        ## TODO: FIX THE DUPLICATE VALUES THAT RESULTS IN AN ERRONOUS JOIN. MIGHT BE MORE OF A FEATURE THAN A BUG.
        print(len(combined_df["device_id"]))
        print(len(weather_df["lat"]))
        print(len(raw_messages_clean_df["device_id"]))

        
        # Save the combined DataFrame to a temporary CSV file
        csv_file_path = '/tmp/raw_messages_cleaned_weather.csv'
        save_df_to_csv(combined_df, csv_file_path)

        # Insert the combined data into the raw_messages_cleaned table
        create_cursor_and_insert_data(conn, csv_file_path, 'raw_messages_cleaned_weather')
        
    finally:
        conn.close()


# Entry point of the script
if __name__ == "__main__":
    main()

