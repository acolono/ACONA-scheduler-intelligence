from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
from acona_pgres_tools import acona_truncate_table, acona_data_write
# [END import_module]


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow'
}
# [END default_args]


# [START instantiate_dag]
@dag(
default_args=default_args,
start_date=days_ago(2),
tags=['prophet'],
schedule_interval='0 5 * * 0')

def acona_forecast_etl():

    # [END instantiate_dag]

    # [START forecast]
    @task()
    def forecast(metric):
        """
        #### Get historic data from Warehouse to generate forecasts
        """

        import json
        import requests
        import os
        import urllib.parse
        import pandas as pd
        import numpy as np
        from prophet import Prophet

        WAREHOUSE_TOKEN = Variable.get("WAREHOUSE_TOKEN")
        WAREHOUSE_URL = Variable.get("WAREHOUSE_URL")

        output = {}
        df = {}
        result = {}

        # Load urls (for specific domain only?)
        urls = os.popen('curl ' + WAREHOUSE_URL + '/rpc/acona_urls -H "Authorization: Bearer ' + WAREHOUSE_TOKEN + '"').read()

        forecasts = {}
        forecasted_lower = pd.DataFrame()
        forecasted_upper = pd.DataFrame()

        for url in json.loads(urls):
            #Load historic data
            url = url['url']
            history = os.popen('curl ' + WAREHOUSE_URL + '/' + metric + '?url=eq.' + url + ' -H "Authorization: Bearer ' + WAREHOUSE_TOKEN + '"').read()
            history = json.loads(history)
            if history and type(history) is list:
              df = pd.DataFrame(history)
              df.rename(
                columns=({ 'date': 'ds', 'value': 'y'}),
                inplace=True,
              )
              df['floor'] = 0
              # Log transform
              df['y'] = np.log(1 + df['y'])
              # Change mode to additive because of log data and use a rather low uncertainty interval.
              m = Prophet(seasonality_mode='additive', interval_width=0.9)
              m.fit(df)
              future = m.make_future_dataframe(periods=7)
              future['floor'] = 0
              forecasted = m.predict(future)
              # Invert transform
              m.history['y'] = np.exp(m.history['y'] - 1)
              for col in ['yhat', 'yhat_lower', 'yhat_upper']:
                value = np.exp(forecasted[col] - 1)
                forecasted[col] = value.round()
              yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
              forecasted = forecasted[(forecasted.ds > yesterday)][['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
              forecasted['url'] = url
              forecasted_lower = forecasted_lower.append(forecasted[['url', 'ds', 'yhat_lower']], ignore_index=True)
              forecasted_upper = forecasted_upper.append(forecasted[['url', 'ds', 'yhat_upper']], ignore_index=True)

        ## Use data to query directly in postgress: we need a table for each table (_f_lower, _f_upper)
        ## Truncate table before because we don't need old forecast values
        #f_lower
        forecasted_lower.rename(columns={'ds': 'date', 'yhat_lower': 'value'}, inplace=True)
        lower_table = 'api.' + metric + '_f_lower'
        acona_truncate_table(lower_table)
        acona_data_write(lower_table, forecasted_lower)
        #f_upper
        forecasted_upper.rename(columns={'ds': 'date', 'yhat_upper': 'value'}, inplace=True)
        upper_table = 'api.' + metric + '_f_upper'
        acona_truncate_table(upper_table)
        acona_data_write(upper_table, forecasted_upper)
    # [END forecast]

    # [START main_flow]

    # Supported metrics. Todo: Load from data warehouse.
    metrics = {
      'metric_d_page_views',
      'metric_d_bounces',
      'metric_d_page_views',
      'metric_d_visits',
      'metric_d_unique_visits',
      'metric_d_conversions',
      'metric_d_visit_time_total',
      'metric_d_visit_time_average',
      'metric_d_visits_converted',
      'metric_d_bounce_rate'
    }

    #metrics = {
      #'metric_d_page_views'
    #}

    # Loop over metrics, forecast values and save in data warehouse
    for metric in metrics:
        forecast(metric)

    # [END main_flow]

# [START dag_invocation]
acona_forecast_etl = acona_forecast_etl()
# [END dag_invocation]
