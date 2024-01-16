import requests
import pandas as pd
from datetime import datetime
import pyarrow.parquet as pq
import pytest

def get_url_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting data from URL: {e}")
        return None

def daily_total_agg(URL_data):
    daily_aggregated = {}

    # TO DO
    # this is not working and needs fixing
    for hourly_data in URL_data['hourly']:
        timestamp = datetime.utcfromtimestamp(hourly_data['timestamp'])
        date_str = timestamp.strftime('%Y-%m-%d')

        if date_str not in daily_aggregated:
            daily_aggregated[date_str] = {
                'temperature_2m': 0,
                'rain': 0,
                'showers': 0,
                'visibility': 0
            }

        daily_aggregated[date_str]['temperature_2m'] += hourly_data.get('temperature_2m', 0)
        daily_aggregated[date_str]['rain'] += hourly_data.get('rain', 0)
        daily_aggregated[date_str]['showers'] += hourly_data.get('showers', 0)
        daily_aggregated[date_str]['visibility'] += hourly_data.get('visibility', 0)

    return daily_aggregated

def output_file_creation(aggregated_data, output_file):
    df = pd.DataFrame.from_dict(aggregated_data, orient='index')
    table = pq.Table.from_pandas(df)
    pq.write_table(table, output_file)

def retrieve_and_aggregate_data():
    # API URL
    api_url = "https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31"

    # Make the API request
    weather_data = get_url_data(api_url)

    if weather_data:
        # Aggregate to daily total
        daily_agg_data = daily_total_agg(weather_data)

        # Output as a parquet file
        output_file = "daily_agg_data.parquet"
        output_file_creation(daily_agg_data, output_file)
        print(f"Parquet file '{output_file}' created successfully.")
    else:
        print("Failed to fetch weather data.")

# Unit tests using pytest
def test_fetch_weather_data():
    assert get_url_data("https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31") is not None

def test_aggregate_daily_total():
    sample_weather_data = {'hourly': [{'timestamp': 1642214400, 'temperature_2m': 20, 'rain': 5, 'showers': 2, 'visibility': 10},
                                      {'timestamp': 1642218000, 'temperature_2m': 22, 'rain': 3, 'showers': 1, 'visibility': 15}]}
    assert daily_total_agg(sample_weather_data) == {'2022-01-15': {'temperature_2m': 42, 'rain': 8, 'showers': 3, 'visibility': 25}}

def test_create_parquet_file(tmp_path):
    sample_data = {'2022-01-15': {'temperature_2m': 42, 'rain': 8, 'showers': 3, 'visibility': 25}}
    output_file = tmp_path / "test.parquet"
    output_file_creation(sample_data, output_file)
    assert output_file.exists()

if __name__ == "__main__":
    retrieve_and_aggregate_data()
