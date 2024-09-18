import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

# Helper function to handle database connections and execute queries
def manage_database(db_url_key, create_table_sql, drop_table_sql=None):
    # Load environment variables from the .env file
    load_dotenv()

    # Fetch the database URL from the .env file
    db_url = os.getenv(db_url_key)

    # Parse the URL to extract connection parameters
    url = urlparse(db_url)

    conn_params = {
        'dbname': url.path[1:],    # Extracts the database name after '/'
        'user': url.username,       # Extracts the username
        'password': url.password,   # Extracts the password
        'host': url.hostname,       # Extracts the host
        'port': url.port            # Extracts the port
    }
    
    # Establish a connection to the database
    conn = psycopg2.connect(**conn_params)
    print(f"Connection to {db_url_key} established successfully!")

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    # Execute the SQL statement to create a table
    cursor.execute(create_table_sql)

    # Commit the transaction to save changes
    conn.commit()
    print("Table created successfully!")

    # Optionally, drop the table if necessary
    if drop_table_sql:
        cursor.execute(drop_table_sql)
        conn.commit()
        print("Table dropped successfully!")

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Function to create staging table
def create_staging_table():
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_messages (
            device_id VARCHAR(255),
            datetime VARCHAR(255),
            address_ip VARCHAR(255),
            address_port INT,
            original_message_id VARCHAR(255),
            raw_message TEXT
        );
    """
    drop_table_sql = "" # "DROP TABLE IF EXISTS raw_messages;" Optional table drop logic
    manage_database("STAGING_KEY", create_table_sql, drop_table_sql)

# Function to create production table
def create_production_table():
    create_table_sql = """
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
    """
    drop_table_sql = "" # "DROP TABLE IF EXISTS raw_messages_cleaned;"  Optional table drop logic
    manage_database("PRODUCTION_KEY", create_table_sql, drop_table_sql)

if __name__ == "__main__":
    create_staging_table()
    create_production_table()
