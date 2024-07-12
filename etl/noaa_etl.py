import os
import argparse
import requests
import pandas as pd
import boto3
from io import StringIO
from typing import Any
from dotenv import load_dotenv

load_dotenv()
AWS_ACESS_KEY_ID = os.getenv("AWS_ACESS_KEY_ID")
AWS_SECRET_ACESS_KEY = os.getenv("AWS_SECRET_ACESS_KEY")
S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACESS_KEY,
    region_name="eu-west-1",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--location_name", type=str, required=True, help="Name of the location."
    )
    return parser.parse_args()


def fetch_city_metadata(name: str) -> dict[str, Any] | None:
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": name.lower(), "format": "json"}
    headers = {"User-Agent": "ETL Project"}
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        try:
            data = response.json()
        except ValueError:
            print("Error: Response is not in JSON format")
            print(f"Response content: {response.text}")
            return None
        if data:
            name = data[0]["name"]
            latitude = round(float(data[0]["lat"]), 4)
            longitude = round(float(data[0]["lon"]), 4)
            return {"name": name, "latitude": latitude, "longitude": longitude}
        else:
            print("Error: No results found")
            return None
    except requests.RequestException as e:
        print(f"Error: Request failed with exception {e}")
        return None


def extract(latitude: str, longitude: str):
    coords_url = f"https://api.weather.gov/points/{latitude},{longitude}"
    api_response = requests.get(coords_url)
    api_response.raise_for_status()
    api_data = api_response.json()
    forecast_url = api_data["properties"]["forecast"]
    forecast_response = requests.get(forecast_url)
    forecast_data = forecast_response.json()
    return forecast_data


def transorm(data) -> dict[str, Any]:
    def fahrenheit_to_celsius(fahrenheit: float) -> float:
        """
        Convert Fahrenheit to Celsius.

        Parameters:
        fahrenheit (float): Temperature in Fahrenheit.

        Returns:
        float: Temperature in Celsius.
        """
        celsius = (fahrenheit - 32) * 5.0 / 9.0
        return celsius

    refined_data = {
        "generatedAt": data["properties"]["generatedAt"],
        "updateTime": data["properties"]["updateTime"],
        "validTimes": data["properties"]["validTimes"],
        "elevation": data["properties"]["elevation"]["value"],
        "periods": [],
    }
    for period in data["properties"]["periods"]:
        period_details = {
            "name": period["name"],
            "temperature": (
                fahrenheit_to_celsius(period["temperature"])
                if period["temperatureUnit"] == "F"
                else period["temperature"]
            ),
            "temperatureUnit": "C",
            "detailedForecast": period["detailedForecast"],
        }
        refined_data["periods"].append(period_details)
    return refined_data


def load(data, name: str):
    df = pd.DataFrame(data)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    try:
        S3_CLIENT.put_object(
            Bucket="weather-logs-project",
            Key=name.lower().replace(" ", "-"),
            Body=csv_buffer.getvalue(),
        )
        print(">>>>> Upload sucessful.")
    except Exception as e:
        print(f"Error: {e}")


def run_noaa_pipeline(location_name: str):
    try:
        metadata = fetch_city_metadata(location_name)
        data = extract(latitude=metadata["latitude"], longitude=metadata["longitude"])
        data = transorm(data)
        load(data=data["periods"], name=metadata["name"])
    except Exception as e:
        print(f"Error: {e}")


def main(args):
    run_noaa_pipeline(args.location_name)


if __name__ == "__main__":
    args = parse_args()
    main(args)
