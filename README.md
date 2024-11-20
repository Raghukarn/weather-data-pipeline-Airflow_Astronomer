# Weather Data Pipeline

This project fetches weather data using a Weather API, processes it with Apache Airflow, and stores it in a PostgreSQL database hosted on AWS RDS.

## Features
- Fetch weather data using latitude and longitude.
- Store data in a PostgreSQL database on AWS RDS.
- Orchestrate the pipeline using Apache Airflow (running on Astronomer).

## Architecture
The workflow is as follows:
1. Fetch weather data using a REST API.
2. Transform and clean the data (if necessary).
3. Load the data into a PostgreSQL database.

## Prerequisites
- Python 3.8+
- PostgreSQL database (hosted on AWS RDS)
- Apache Airflow installed (Astronomer or local setup)

## Setup
1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/weather-data-pipeline.git
   cd weather-data-pipeline

## Running the Pipeline
- Start the Airflow scheduler and webserver.
- Deploy the DAG to your Airflow dags/ folder.
- Trigger the DAG from the Airflow web interface.

## Technologies Used
- Python
- Apache Airflow
- PostgreSQL (AWS RDS)
- REST APIs
