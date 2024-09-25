import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from exploratory_data_analysis import fetch_data_from_db, robust_clean_raw_message

class ExploratoryDataAnalysisTestCase(unittest.TestCase):

    @patch('exploratory_data_analysis.psycopg2.connect')
    def test_fetch_data_from_db(self, mock_connect):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock query result and description for pandas to process
        mock_cursor.fetchall.return_value = [(1, 'st-1a2090', 123456789, 'ip', 1234, 'raw_message')]
        mock_cursor.description = [
            ('device_id',), ('datetime',), ('address_ip',), ('address_port',), ('raw_message',)
        ]
        
        # Expected DataFrame result
        expected_df = pd.DataFrame({
            'device_id': ['st-1a2090'],
            'datetime': [123456789],
            'address_ip': ['ip'],
            'address_port': [1234],
            'raw_message': ['raw_message']
        })
        
        # Call fetch_data_from_db function
        result_df = fetch_data_from_db(query="SELECT * FROM raw_messages;", environment="STAGING")
        
        # Validate the result DataFrame
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_robust_clean_raw_message_valid(self):
        # Test with a valid message
        test_message = "A,51.31831,N,4.18015,E,0.0,1.59,150218,0.8,W"
        result = robust_clean_raw_message(test_message)
        
        expected_result = {
            'data_status': 'A',
            'latitude': 51.31831,
            'latitude_direction': 'N',
            'longitude': 4.18015,
            'longitude_direction': 'E',
            'speed_over_ground_d': 0.0,
            'true_course': 1.59,
            'ut_date': 150218.0,
            'mag_var_d': 0.8,
            'mag_var_dir': 'W'
        }
        
        self.assertEqual(result, expected_result)

    def test_robust_clean_raw_message_invalid(self):
        # Test with an invalid message
        test_message = "Invalid message"
        result = robust_clean_raw_message(test_message)
        self.assertIsNone(result)
    
    def test_robust_clean_raw_message_edge_case(self):
        # Test with a message that has missing fields
        test_message = "A,51.31831,N"
        result = robust_clean_raw_message(test_message)
        self.assertIsNone(result)

if __name__ == "__main__":
    unittest.main()
