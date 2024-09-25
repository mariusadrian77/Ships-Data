import unittest
from unittest.mock import patch, MagicMock
from db_creation import create_staging_table, create_production_table, create_production_table_2, manage_database

class DBCreationTestCase(unittest.TestCase):
    
    @patch('db_creation.psycopg2.connect')
    def test_manage_database_create_table(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255)
        );
        """
        
        manage_database("STAGING_KEY", create_table_sql)
        
        # Ensure connection is established
        mock_connect.assert_called_once()
        
        # Ensure the table creation SQL is executed
        mock_cursor.execute.assert_called_with(create_table_sql)
        
        # Ensure the transaction is committed
        mock_conn.commit.assert_called_once()

        # Ensure cursor and connection are closed
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('db_creation.manage_database')
    def test_create_staging_table(self, mock_manage_db):
        # Test if the correct SQL statement is passed for staging table creation
        create_staging_table()
        mock_manage_db.assert_called_with("STAGING_KEY", unittest.mock.ANY, "")

    @patch('db_creation.manage_database')
    def test_create_production_table(self, mock_manage_db):
        # Test if the correct SQL statement is passed for production table creation
        create_production_table()
        mock_manage_db.assert_called_with("PRODUCTION_KEY", unittest.mock.ANY, "")

    @patch('db_creation.manage_database')
    def test_create_production_table_2(self, mock_manage_db):
        # Test if the correct SQL statement is passed for production table 2 creation
        create_production_table_2()
        mock_manage_db.assert_called_with("PRODUCTION_KEY", unittest.mock.ANY, "")
    
if __name__ == '__main__':
    unittest.main()
