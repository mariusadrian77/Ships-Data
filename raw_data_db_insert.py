import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

def copy_from_csv(cursor, file_path, table_name):
    with open(file_path, 'r') as f:
        next(f)  # Skip the header row
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)

def create_cursor_and_insert_data(connection, csv_file_path, table_name):
    cursor = connection.cursor()

    print("Connection established successfully!")
    
    # Use COPY command to bulk insert from CSV
    copy_from_csv(cursor, csv_file_path, table_name)

    print("Data inserted successfully!")

    # Commit the transaction
    connection.commit()
    
    # Close the cursor
    cursor.close()

# Main entry point of the script
def main():

    load_dotenv()

    # Fetch the database URL from the .env file
    STAGING_URL = os.getenv("STAGING_KEY")

    # Parse the URL to extract connection parameters
    url = urlparse(STAGING_URL)

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
        # Path to the CSV file
        csv_file_path = '/workspaces/Xomnia-Assignment/data/raw_messages.csv'
        
        # Create the tables and insert the data
        create_cursor_and_insert_data(conn, csv_file_path, 'raw_messages')
    finally:
        conn.close()


# Entry point of the script
if __name__ == "__main__":
    main()

