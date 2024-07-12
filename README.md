# National Oceanic and Atmospheric Administration Data Pipeline

![Architecture](architecture.png)

This project involves fetching weather reports based on location names, processing the data, and storing it in Amazon S3. The architecture diagram for this pipeline is included in this repository.

## Architecture

The pipeline consists of the following steps:

1. *Fetch Coordinates*: Using the OpenStreetMap API, fetch the latitude and longitude for a given location name.
2. *Fetch Weather Data*: Use the coordinates obtained in the previous step to fetch weather data from the NOAA API.
3. *ETL Process*: Run an Apache Airflow ETL pipeline on an Amazon EC2 instance. The ETL process includes: extracting weather data from the NOAA API, transforming the data by selecting specific features and converting temperatures to Celsius, loading the transformed data into an Amazon S3 bucket.
4. *Data Transformation*: Select specific features from the API response and convert temperatures from Fahrenheit to Celsius.
5. *Store Data*: The transformed data is stored in an Amazon S3 bucket for further analysis and use.

## Prerequisites
- AWS account with EC2 and S3 access
- Apache Airflow installed on EC2
- Python 3.11.9



   
