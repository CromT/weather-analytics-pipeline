import os
import requests
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account

# --- 1. SETTINGS & AUTHENTICATION ---

# IMPORTANT: This tells the script to find the credentials file you downloaded.
# Make sure 'credentials.json' is in the SAME folder as this script.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

# --- !! REPLACE THESE VALUES !! ---

# 1. Get this from your Google Cloud project (from the screenshot)
project_id = "weather-pipeline-475921"

# 2. This is the dataset.tablename you created in BigQuery
table_id = "weather_data.daily_readings"

# 3. Get this from your OpenWeatherMap account
api_key = "ea674572b90d9467adb263de09f856b5"

# 4. Change this to any city you want to track
city = "Boston"

# --- (End of values to replace) ---


# This builds the API request URL
url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"


def run_weather_pipeline():
    # --- Load credentials explicitly from the file ---
    # This forces the script to use your service account
    credentials = service_account.Credentials.from_service_account_file(
        'credentials.json')  # <--- NEW LINE

    # --- 2. EXTRACT (E) ---
    print(f"Requesting data from OpenWeatherMap for {city}...")
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print("Data extracted successfully.")

        # --- 3. TRANSFORM (T) ---
        transformed_data = {
            'timestamp': [pd.to_datetime(data['dt'], unit='s')],
            'city': [data['name']],
            'temperature': [data['main']['temp']],
            'humidity': [data['main']['humidity']],
            'weather_description': [data['weather'][0]['description']]
        }

        df = pd.DataFrame(transformed_data)

        print("Data transformed into DataFrame:")
        print(df.to_string())

        # --- 4. LOAD (L) ---
        print(f"Loading data into BigQuery table {table_id}...")

        # We now pass the credentials directly into the function
        to_gbq(df,
               destination_table=table_id,
               project_id=project_id,
               if_exists='append',
               credentials=credentials)  # <--- UPDATED LINE

        print("--- SUCCESS ---")
        print("Data loaded successfully into BigQuery.")

    else:
        print(f"Error: Failed to get data. Status code: {response.status_code}")
        print(f"Response: {response.text}")

# This makes the script runnable
if __name__ == "__main__":
    run_weather_pipeline()