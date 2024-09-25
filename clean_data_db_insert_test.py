import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from clean_data_db_insert import filter_raw_messages_clean_df, load_weather_data, filter_weather_data, save_df_to_csv

class CleanDataDBInsertTestCase(unittest.TestCase):

    @patch('clean_data_db_insert.psycopg2.connect')
    def test_filter_raw_messages_clean_df(self, mock_connect):
        # Create a sample raw messages DataFrame
        sample_data = {
            'device_id': ['st-1a2090'],
            'datetime': [1550066999],  # String format datetime as per table structure
            'address_ip': ['192.168.0.1'],
            'address_port': [1234],
            'original_message_id': ['msg_001'],
            'raw_message': ["A,51.31830816666667,N,4.315722166666666,E,0.0,1.59,150218,0.8,E"]
        }
        sample_df = pd.DataFrame(sample_data)

        # Call the function to filter and clean the raw messages
        result_df = filter_raw_messages_clean_df(sample_df)

        # Expected result for raw_messages_cleaned structure
        expected_data = {
            'device_id': ['st-1a2090'],
            'datetime': [pd.to_datetime(1550066999)],
            'address_ip': ['192.168.0.1'],
            'address_port': [1234],
            'original_message_id': ['msg_001'],
            'data_status': ['A'],
            'latitude': [51.32],
            'latitude_direction': ['N'],
            'longitude': [4.18],
            'longitude_direction': ['E'],
            'speed_over_ground_d': [0.0],
            'true_course': [1.59],
            'ut_date': [150218.0],
            'mag_var_d': [0.8],
            'mag_var_dir': ['W']
        }
        expected_df = pd.DataFrame(expected_data)

        # Ensure both DataFrames have the same columns
        self.assertEqual(list(result_df.columns), list(expected_df.columns))

        # Assert that the result matches the expected DataFrame
        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch('clean_data_db_insert.open')
    @patch('clean_data_db_insert.json.load')
    def test_load_weather_data(self, mock_json_load, mock_open):
        # Sample weather data
        sample_weather_data = {
            "data": [
                {
                    "lat": 51.32,
                    "lon": 4.18,
                    "city_name": "CityA",
                    "station_id": "ST-001",
                    "timezone": "Europe/Brussels",
                    "datetime": "2019-02-13 01:00:00",
                    "temp": 12.3,
                    "rh": 70.0,
                    "wind_spd": 5.0
                }
            ]
        }
        
        # Mock the return value of json.load
        mock_json_load.return_value = sample_weather_data
        
        # Call the load_weather_data function
        weather_df = load_weather_data("/workspaces/Xomnia-Assignment/data/fake_path.json")
        
        # Expected DataFrame for raw_messages_cleaned_weather structure
        expected_weather_data = {
            'lat': [51.32],
            'lon': [4.18],
            'city_name': ['CityA'],
            'station_id': ['ST-001'],
            'timezone': ['Europe/Brussels'],
            'datetime': ['2019-02-13 01:00:00'],
            'temp': [12.3],
            'rh': [70.0],
            'wind_spd': [5.0]
        }
        expected_df = pd.DataFrame(expected_weather_data)
        
        # Assert that the result matches the expected DataFrame
        pd.testing.assert_frame_equal(weather_df, expected_df)

    def test_filter_weather_data(self):
        # Sample weather data before filtering
        sample_data = {
            'lat': [51.32],
            'lon': [4.18],
            'datetime': ['2019-02-13 01:00:00'],
            'temp': [12.3],
            'rh': [70.0],
            'wind_spd': [5.0]
        }
        sample_df = pd.DataFrame(sample_data)

        # Call the filter_weather_data function
        result_df = filter_weather_data(sample_df)

        # Expected DataFrame after filtering and converting datetime
        expected_data = {
            'lat': [51.32],
            'lon': [4.18],
            'datetime': [pd.to_datetime('2019-02-13 01:00:00')],
            'temp': [12.3],
            'rh': [70.0],
            'wind_spd': [5.0]
        }
        expected_df = pd.DataFrame(expected_data)

        # Assert that the result matches the expected DataFrame
        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch('clean_data_db_insert.open')
    def test_save_df_to_csv(self, mock_open):
        # Create a sample DataFrame
        sample_data = {'col1': [1, 2], 'col2': [3, 4]}
        sample_df = pd.DataFrame(sample_data)

        # Call the save_df_to_csv function
        save_df_to_csv(sample_df, '/workspaces/Xomnia-Assignment/datafake_path.csv')

        # Ensure the file was opened
        mock_open.assert_called_with('/workspaces/Xomnia-Assignment/datafake_path.csv', 'w')

if __name__ == "__main__":
    unittest.main()
