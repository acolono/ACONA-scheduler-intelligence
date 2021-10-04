#ACONA Scheduler and Intelligence Suite

This component is responsible to write data into the ACONA Data Warehouse. It schedules jobs and provides functionality 
to calculate scores, recommendations, forecasts and alerts.

Scheduling is done with Apache Airflow: https://airflow.apache.org/

For forecasts Prophet is used, an Open Source software by Facebook: https://facebook.github.io/prophet/

## About ACONA

ACONA stands for Augmented Content Analytics - an open source tool that automatically analyzes and simplifies data, for example from server logs or existing (open source) analytics tools, and proposes concrete measures for optimizing content.

More in the general documentation: https://app.gitbook.com/@acolono/s/acona/

## How to use
Build the docker image
```
docker build . -f Dockerfile --tag acona-airflow:0.0.1
```
Rename .env_template to .env, create a fernet key and update values in .env.

How to generate a fernet key: https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html

Run docker-compose
```
docker-compose up
```

Before you can run a dag you need to create the following variables:
- WAREHOUSE_URL: The ACONA Warehouse URL you want to use
- WAREHOUSE_TOKEN: The authentication token for the ACONA Warehouse
and 
- MATOMO_URL and MATOMO_TOKEN for every configured domain like this:
When your Domain ID is 1 name the variables MATOMO_URL_1 and MATOMO_TOKEN_1

## ACONA Data Warehouse
To set up your own ACONA Data Warehouse, see https://github.com/acolono/ACONA-data-warehouse 



