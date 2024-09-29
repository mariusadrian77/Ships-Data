# Xomnia Assignment Instructions

This readme file contains instructions on how to use the designed solution out-of-the-box.

## Steps to Get Started

### 1. Access the PostgreSQL Database
Use the following invite link to get access to the PostgreSQL database hosted on Railway:
[Railway invite link](https://railway.app/invite/Gya37GNkNZg)

### 2. Create the Database Schema and Table
Run the `db_creation.py` Python script to create the database schema and table. You might need to create a `.env` file containing the connection string with the database credentials.

> **Note:** For the sake of the project, I have made the `.env` file available (although this is not best practice). Alternatively, you can find the database credentials by following these steps:
- Navigate to the deployment.
- Click the PostgreSQL database.
- A window will open on the right side of the screen.
- Click the "Data" section where you can see the table(s).
- Above the table, there is a panel with "Connect to the database Postgres".
- Press the "Connect" button on the right side of the panel.
- A new window will open, then press the "Public Network" section.
- Click the copy button next to the "Connection URL" section.
- Paste the connection URL into your `.env` file as:
  ```ini
  DATABASE_KEY = '{connection_url}'

### 3. Run the `raw_data_db_insert.py` and `clean_data_db_insert.py` Python Scripts
#### 
- The `raw_data_db_insert.py` script will insert the `raw_messages.csv` file into the `STAGING` environment of the database. This will act as the "bronze" layer data.
- The `clean_data_db_insert.py` script will fetch the previously uploaded `raw_messages.csv` file from the `STAGING` environment of the database. Then it will clean the dataset and bring it to the same structure as the given `raw_messages_clean.csv` file. Then it will load the `weather_data.json` file, clean it and combine it with the `raw_messages_clean` dataset based on time and location.
- Lastly, it will copy the data to the `PRODUCTION` environment of the database. The `raw_messages_clean` dataset will represent the "silver" data layer, while the `combined` dataset will represent the "gold" layer.

### 4. Run the `app.py` Python Script
- This script will connect to the PostgreSQL database, where the datasets were inserted, specifically to the `PRODUCTION` environment and fetch the combined dataset for further analysis. It will calculate the following metrics:
  1. The number of ships that we have available data for.
  2. The average speed for all available ships for each hour of the date 2019-02-13.
  3. The maximum and minimum wind speed for every available day for ship ”st-1a2090” only.
  4. The full weather conditions across route of the ship ”st-1a2090” for date 2019-02-13 (e.g. general description, temperature, wind
speed)
  
- The web application will be deployed on port `5000` and can be accessed via the web browser at:
[Web API](http://127.0.0.1:5000/)


- The previously calculated metrics can be viewed at:
[Total ships](http://127.0.0.1:5000/metrics/total_ships), 
[Average speed](http://127.0.0.1:5000/metrics/avg_speed), 
[Wind speed](http://127.0.0.1:5000/metrics/wind_speed), 
[Weather conditions](http://127.0.0.1:5000/metrics/weather_conditions).


### Troubleshooting
If any problems arise during the database creation step, you can modify lines 61, 86 and 140 of `db_creation.py` to delete the table and retry the steps.

To do this, uncomment the "DROP TABLE" statement and add it between the quotation marks, for example:

```python
drop_table_sql = "DROP TABLE IF EXISTS webshop_events;"  # Optional table drop logic
