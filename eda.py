import pandas as pd
import re


messages_clean = pd.read_csv("/workspaces/Xomnia-Assignment/raw_messages_clean.csv")
raw_messages_df = pd.read_csv("/workspaces/Xomnia-Assignment/raw_messages.csv")

# print(messages_clean.head())

# raw_messages_info = raw_messages_df.info()
# raw_messages_head = raw_messages_df.head()

# raw_messages_info,raw_messages_head

# Check for missing values in the dataset
missing_values = raw_messages_df.isnull().sum()

# Convert Unix timestamps to a readable datetime format
raw_messages_df['datetime'] = pd.to_datetime(raw_messages_df['datetime'], unit='s')

# Explore the distribution of device IDs
device_id_counts = raw_messages_df['device_id'].value_counts()

# print(missing_values, raw_messages_df['datetime_converted'].head(), device_id_counts.head())



# Updated cleaning function with more robust error handling
def robust_clean_raw_message(message):
    # Remove any characters that are not part of a basic valid set (alphanumeric, commas, dots, and basic direction letters)
    clean_message = re.sub(r'[^A-Za-z0-9.,NSWE]', '', message)
    
    # Split the cleaned message by commas
    parts = clean_message.split(',')
    
    # Basic validation: latitude and longitude should be present (at least 7 fields to attempt extraction)
    if len(parts) >= 7:
        # Attempt to extract latitude, longitude, and speed fields even if minor noise is present
        try:
            data_status = parts[0]  # 'A' or 'V'
            latitude = float(parts[1])  # Latitude as a float
            latitude_dir = parts[2]  # 'N' or 'S'
            longitude = float(parts[3])  # Longitude as a float
            longitude_dir = parts[4]  # 'E' or 'W'
            speed_over_ground_d = float(parts[5])  # Speed over ground
            true_course = float(parts[6])  # Track made good (degrees True)
            ut_date = float(parts[7])
            mag_var_d = float(parts[8])
            mag_var_dir = parts[9]

            
            # Reconstruct a clean message with the necessary fields
            return {
                'data_status': data_status,
                'latitude': latitude,
                'latitude_direction': latitude_dir,
                'longitude': longitude,
                'longitude_direction': longitude_dir,
                'speed_over_ground_d': speed_over_ground_d,
                'true_course': true_course,
                'ut_date': ut_date,
                'mag_var_d': mag_var_d,
                'mag_var_dir': mag_var_dir
            }
        except (ValueError, IndexError):
            # If there's any issue with conversion or missing data, return None (invalid message)
            return None
    else:
        return None

# Apply the robust cleaning function
raw_messages_df['cleaned_message'] = raw_messages_df['raw_message'].apply(robust_clean_raw_message)

# Check the first few successfully cleaned rows
cleaned_sample_robust = raw_messages_df[['raw_message', 'cleaned_message']].head(10)

# Count how many messages were successfully cleaned now
valid_messages_count_robust = raw_messages_df['cleaned_message'].notnull().sum()
invalid_messages_count_robust = raw_messages_df['cleaned_message'].isnull().sum()

#print(cleaned_sample_robust, valid_messages_count_robust, invalid_messages_count_robust)

# Now, we will expand the dictionary returned by 'cleaned_fields' into separate columns
cleaned_columns_df = pd.json_normalize(raw_messages_df['cleaned_message'])

# Concatenate the cleaned columns back to the original dataframe
raw_messages_cleaned_df = pd.concat([raw_messages_df, cleaned_columns_df], axis=1)

# Remove the 'raw_message' and 'cleaned_message' columns
raw_messages_cleaned_df = raw_messages_cleaned_df.drop(columns=['raw_message', 'cleaned_message'])

# Display the first few rows of the cleaned dataframe
print(raw_messages_cleaned_df.head())
