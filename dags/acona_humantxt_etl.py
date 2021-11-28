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
tags=['etl', 'humantxt'],
schedule_interval='30 5 * * 0')

def acona_humantxt():

    # [END instantiate_dag]

    # [START notify]
    @task()
    def ht_extract(domain):
        """
        #### Get data for current date from humantxt API
        """

        import json
        import requests
        import os
        import urllib.parse
        import pandas as pd
        import re

        WAREHOUSE_TOKEN = Variable.get("WAREHOUSE_TOKEN")
        WAREHOUSE_URL = Variable.get("WAREHOUSE_URL")

        # Load urls for domain
        sql = "select url from internal.urls where domain_id = {} and status = 't'".format(domain[1])
        urls = acona_fetch_data(sql)
        #[('https://www.acona.app/about',), ('https://www.acona.app/metrics',), ('https://www.acona.app/legal',), ('https://www.acona.app/info',), ('https://www.acona.app/',)]

        date = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
        #date = '2021-10-22 18:45:01'

        for url in urls:
            url = url[0]

            HEADERS = {
                'Content-Type': 'application/json; charset=utf-8'
            }
            HUMANTXT_URL = 'https://humantxt.acona.app/'

            #URL = urllib.parse.quote(url, safe='')
            request_url = HUMANTXT_URL + '?url=' + url + '&format=html'

            r = requests.get(request_url, headers=HEADERS)
            result = False
            if r.status_code == 200:
                result = r.json()

            if result and result.get('error') != 'true':
                # title
                if result.get('title'):
                    value = result.get('title')
                else:
                    value = ''

                table = 'api.var_ht_title'
                data = {'value': [value], 'datetime': [date], 'url': [url]}
                dataf = pd.DataFrame(data)
                acona_data_write(table, dataf)

                # title char count
                if result.get('title'):
                    value = len(result.get('title'))
                else:
                    value = 0

                table = 'api.var_ht_title_char_count'
                data = {'value': [value], 'datetime': [date], 'url': [url]}
                dataf = pd.DataFrame(data)
                acona_data_write(table, dataf)

                # content
                if result.get('content'):
                    value = result.get('content')
                else:
                    value = 0

                table = 'api.var_ht_content_html'
                data = {'value': [value], 'datetime': [date], 'url': [url]}
                dataf = pd.DataFrame(data)
                acona_data_write(table, dataf)

                # excerpt
                if result.get('excerpt'):
                    value = result.get('excerpt')
                else:
                    value = 0

                table = 'api.var_ht_excerpt'
                data = {'value': [value], 'datetime': [date], 'url': [url]}
                dataf = pd.DataFrame(data)
                acona_data_write(table, dataf)

                # word count
                if result.get('word_count'):
                    value = result.get('word_count')
                else:
                    value = 0

                table = 'api.var_ht_word_count'
                data = {'value': [value], 'datetime': [date], 'url': [url]}
                dataf = pd.DataFrame(data)
                acona_data_write(table, dataf)

                # write import dates
                data = {'variable': ['humantxt'], 'date': [date], 'url': [url]}
                dataf = pd.DataFrame(data)
                acona_data_write('internal.var_calc_dates', dataf)

    # [END extract]

    # [START main_flow]

    # Load domains
    sql = "select domain_name, domain_id from internal.domains where status = 't'"
    domains = acona_fetch_data(sql)

    # Loop over domains
    if domains:
        for domain in domains:
            ht_extract(domain)

    # [END main_flow]

# [START dag_invocation]
acona_humantxt = acona_humantxt()
# [END dag_invocation]
