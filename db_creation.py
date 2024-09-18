import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

def create_tables_and_indexes():
    # Load environment variables from the .env file
    load_dotenv()

    # Fetch the database URL from the .env file
    POSTGRESQL_URL = os.getenv("POSTGRESQL_KEY")

    # Parse the URL to extract connection parameters
    url = urlparse(POSTGRESQL_URL)

    conn_params = {
        'dbname': url.path[1:],    # Extracts the database name after '/'
        'user': url.username,       # Extracts the username
        'password': url.password,   # Extracts the password
        'host': url.hostname,       # Extracts the host
        'port': url.port            # Extracts the port
    }
    
    # Establish a connection to the database
    conn = psycopg2.connect(**conn_params)

    print("Connection established successfully!")

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    # Example: Create a table for the raw data (bronze data)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_messages (
            device_id VARCHAR(255),
            datetime VARCHAR(255),
            address_ip VARCHAR(255),
            address_port INT,
            original_message_id VARCHAR(255),
            raw_message TEXT
        );
    """)

    # Example: Create a table for the cleaned date (silver data)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_messages_cleaned (
            device_id VARCHAR(255),
            datetime TIMESTAMP,
            address_ip VARCHAR(255),
            address_port INT,
            original_message_id VARCHAR(255),
            data_status CHAR(1),
            latitude DECIMAL,
            latitude_direction CHAR(1),
            longitude DECIMAL,
            longitude_direction CHAR(1),
            speed_over_ground_d DECIMAL,
            true_course DECIMAL,
            ut_date DECIMAL,
            mag_var_d DECIMAL,
            mag_var_dir CHAR(1)
        );
    """)

    # Commit the transaction to save changes
    conn.commit()

    print("Tables created successfully!")

    # # Uncomment the lines below to drop the tables if necessary
    # cursor.execute("DROP TABLE IF EXISTS raw_messages;")
    # cursor.execute("DROP TABLE IF EXISTS raw_messages_cleaned;") 

    # # Commit the changes
    # conn.commit()

    # print("Tables deleted successfully!")

    # Close the cursor and connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_tables_and_indexes()