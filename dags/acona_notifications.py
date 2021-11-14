from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
from acona_postgres_tools import acona_truncate_table, acona_data_write, acona_fetch_data, acona_fetch_one
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
tags=['notifications'],
schedule_interval='30 4 * * 0')

def acona_notifications():

    # [END instantiate_dag]

    # [START notify]
    @task()
    def notify(metric):
        """
        #### Get data for current date from Warehouse
        """

        import json
        import requests
        import os
        import urllib.parse
        import pandas as pd
        import numpy as np
        import re

        WAREHOUSE_TOKEN = Variable.get("WAREHOUSE_TOKEN")
        WAREHOUSE_URL = Variable.get("WAREHOUSE_URL")

        # Load urls (for specific domain only?)
        urls = os.popen('curl ' + WAREHOUSE_URL + '/rpc/acona_urls -H "Authorization: Bearer ' + WAREHOUSE_TOKEN + '"').read()

        notifications = {}

        date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        #date = '2021-10-22'

        #Load metric data
        sql = "select * from internal.variables where var_data_table = '{}'".format(metric)
        values = acona_fetch_one(sql)
        metric_id = values[0]
        #TODO: Add metric title to internal.variables table and use it here.
        metric_mn = values[1]

        for url in json.loads(urls):
            #Load historic data
            url = url['url']
            sql = "select value from api." + metric + " where date = '{}' and url = '{}'".format(date, url)
            values = acona_fetch_one(sql)
            if values:
                value = values[0]
                # Load forecasted upper value.
                sql = "select value from api." + metric + "_f_upper where date = '{}' and url = '{}'".format(date, url)
                upper_values = acona_fetch_one(sql)
                upper_value = None
                if upper_values:
                    upper_value = upper_values[0]
                # Load forecasted lower value.
                sql = "select value from api." + metric + "_f_lower where date = '{}' and url = '{}'".format(date, url)
                lower_values = acona_fetch_one(sql)
                lower_value = None
                if lower_values:
                    lower_value = lower_values[0]
                # Compare values.
                notification_id = re.sub('[^A-Za-z0-9]+', '', str(date)) + '_' + re.sub('[^A-Za-z0-9]+', '', str(url)) + str(metric) + '_lower'
                if lower_value != None and value < lower_value:
                    #write notification.
                    data = {'notification_id': [notification_id], 'url': [url], 'date': [date], 'variable_id': [metric_id]}
                    dataf = pd.DataFrame(data)
                    acona_data_write('api.notifications', dataf)
                    text_en = 'Attention, the value for ' + metric_mn + ' is lower than expected. Please check if something is wrong.'
                    title_en = 'Value is lower as expected'
                    data = {'notification_id': [notification_id], 'langcode': ['en'], 'title': [title_en], 'text': [text_en]}
                    dataf = pd.DataFrame(data)
                    acona_data_write('api.notification_texts', dataf)
                if upper_value != None and value > upper_value:
                    #write notification.
                    data = {'notification_id': [notification_id], 'url': [url], 'date': [date], 'variable_id': [metric_id]}
                    dataf = pd.DataFrame(data)
                    acona_data_write('api.notifications', dataf)
                    text_en = 'Attention, the value for ' + metric_mn + ' is higher than expected.'
                    title_en = 'Value is higher as expected'
                    data = {'notification_id': [notification_id], 'langcode': ['en'], 'title': [title_en], 'text': [text_en]}
                    dataf = pd.DataFrame(data)
                    acona_data_write('api.notification_texts', dataf)

                # write calc dates
                data = {'variable': 'api.notifications', 'date': date, 'url': url}
                dataf = pd.DataFrame(data)
                acona_data_write('internal.var_calc_dates', dataf)
    # [END notify]

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
        notify(metric)

    # [END main_flow]

# [START dag_invocation]
acona_notifications = acona_notifications()
# [END dag_invocation]
