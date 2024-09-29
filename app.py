import os
import psycopg2
import pandas as pd
from flask import Flask, jsonify, request, send_file
from urllib.parse import urlparse
from dotenv import load_dotenv
from exploratory_data_analysis import fetch_data_from_db
import matplotlib.pyplot as plt
from io import BytesIO

app = Flask(__name__)

# Welcome Page with a list of available metrics as JSON
@app.route('/')
def welcome():
    return jsonify({
        "message": "Welcome to the Ship Metrics API",
        "available_metrics": [
            {"metric": "Total Ships", "description": "Total number of ships", "endpoint": "/metrics/total_ships"},
            {"metric": "Average Speed for st-1a2090", "description": "Average speed for ship 'st-1a2090' for a specific hour on 2019-02-13", "endpoint": "/metrics/avg_speed"},
            {"metric": "Max/Min Wind Speed for st-1a2090", "description": "Maximum and minimum wind speeds for each day for ship 'st-1a2090'", "endpoint": "/metrics/wind_speed"},
            {"metric": "Weather Conditions for st-1a2090 on 2019-02-13", "description": "Weather conditions for ship 'st-1a2090' on 2019-02-13", "endpoint": "/metrics/weather_conditions"}
        ]
    })

# Endpoint 1: Total number of ships
@app.route('/metrics/total_ships', methods=['GET'])
def total_ships():
    total_ships_count = raw_messages_cleaned_weather_df['device_id'].nunique()
    return jsonify({"total_ships": total_ships_count})

# Endpoint 2: Average speed for the ship "st-1a2090" for a specific hour on 2019-02-13
@app.route('/metrics/avg_speed', methods=['GET'])
def avg_speed():
    # Fetch date and hour from request (or set default to 2019-02-13 14:00:00)
    requested_datetime = request.args.get('datetime', '2019-02-13 14:00:00')
    requested_datetime = pd.to_datetime(requested_datetime)

    # Filter for the specific ship and hour
    filtered_df = raw_messages_cleaned_weather_df[
        (raw_messages_cleaned_weather_df['device_id'] == 'st-1a2090') &
        (raw_messages_cleaned_weather_df['datetime'] == requested_datetime)
    ]
    
    # Calculate the average speed
    avg_speed_value = filtered_df['speed_over_ground_d'].mean()
    
    return jsonify({"avg_speed": avg_speed_value})

# Endpoint 3: Maximum and minimum wind speeds for each day for ship "st-1a2090"
@app.route('/metrics/wind_speed', methods=['GET'])
def wind_speed():
    # Filter for ship "st-1a2090"
    filtered_df = raw_messages_cleaned_weather_df[raw_messages_cleaned_weather_df['device_id'] == 'st-1a2090']
    
    # Group by date and calculate max and min wind speeds
    filtered_df['date'] = filtered_df['datetime'].dt.date
    wind_speed_stats = filtered_df.groupby('date')['wind_spd'].agg(['max', 'min']).reset_index()
    
    wind_speed_stats_dict = wind_speed_stats.to_dict(orient='records')
    
    return jsonify(wind_speed_stats_dict)

# Endpoint 4: Weather conditions for ship "st-1a2090" on 2019-02-13
@app.route('/metrics/weather_conditions', methods=['GET'])
def weather_conditions():
    # Filter for ship "st-1a2090" and date 2019-02-13
    requested_date = pd.to_datetime('2019-02-13').date()
    filtered_df = raw_messages_cleaned_weather_df[
        (raw_messages_cleaned_weather_df['device_id'] == 'st-1a2090') &
        (raw_messages_cleaned_weather_df['datetime'].dt.date == requested_date)
    ]

    # Select weather-related columns
    weather_info = filtered_df[[
        'temp', 'wind_spd', 'rh', 'weather_description', 'city_name', 'timezone'
    ]].drop_duplicates().to_dict(orient='records')

    return jsonify(weather_info)


if __name__ == '__main__':

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

    # Load the data into raw_messages_cleaned_weather_df
    raw_messages_cleaned_weather_df = fetch_data_from_db(query="SELECT * FROM raw_messages_cleaned_weather;", environment="PRODUCTION")

    # Run the Flask app
    app.run(debug=True)
