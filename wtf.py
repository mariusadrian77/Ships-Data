import pandas as pd
import json

# Load the CSV file (raw_messages_clean)
csv_file_path = '/workspaces/Xomnia-Assignment/raw_messages_clean.csv'
raw_messages_clean_df = pd.read_csv(csv_file_path)

# Load the JSON file (weather_data)
json_file_path = '/workspaces/Xomnia-Assignment/weather_data.json'
with open(json_file_path, 'r') as json_file:
    weather_data = json.load(json_file)

# Normalize the JSON data (weather_data)
weather_df = pd.json_normalize(weather_data, 'data', ['lat', 'lon', 'city_name', 'station_id', 'timezone'])

# Convert datetime fields in both DataFrames to ensure they are in the same format
# Clean the 'datetime' column by replacing ":" with a space and appending ":00" where needed
weather_df['datetime'] = weather_df['datetime'].str.replace(':', ' ', regex=False) + ':00'
weather_df['datetime'] = pd.to_datetime(weather_df['datetime'], errors='coerce')


# Filter raw_messages_clean_df
raw_messages_clean_df = raw_messages_clean_df.rename(columns={"latitude": "lat", "longitude": "lon"})
raw_messages_clean_df = raw_messages_clean_df[raw_messages_clean_df['device_id'] == "st-1a2090"]
raw_messages_clean_df['datetime'] = pd.to_datetime(raw_messages_clean_df['datetime'], errors='coerce')
raw_messages_clean_df['datetime'] = raw_messages_clean_df['datetime'].dt.round('H')
raw_messages_clean_df = raw_messages_clean_df[raw_messages_clean_df['datetime'].dt.date == pd.to_datetime("2019-02-13").date()]
raw_messages_clean_df['lat'] = raw_messages_clean_df['lat'].astype(float).round(2)
raw_messages_clean_df['lon'] = raw_messages_clean_df['lon'].astype(float).round(2)


# Filter weather_df
weather_df['datetime'] = pd.to_datetime(weather_df['datetime'], errors='coerce')
weather_df['datetime'] = weather_df['datetime'].dt.round('H')
weather_df['lat'] = weather_df['lat'].astype(float).round(2)
weather_df['lon'] = weather_df['lon'].astype(float).round(2)


print(raw_messages_clean_df[['datetime', 'lat', 'lon']])
print(weather_df[['datetime', 'lat', 'lon']])


# Merge raw_messages_clean_df with weather_df on 'datetime', 'latitude', and 'longitude'
combined_df = pd.merge(
    raw_messages_clean_df, 
    weather_df, 
    how='left', 
    left_on=['datetime', 'lat', 'lon'], 
    right_on=['datetime', 'lat', 'lon']
)

print(combined_df.head())

# Save the combined DataFrame to a CSV file
combined_df.to_csv('/workspaces/Xomnia-Assignment/combined_data.csv', index=False)
# weather_df.to_csv('/workspaces/Xomnia-Assignment/weather.csv', index=False)


