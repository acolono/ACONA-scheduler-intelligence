from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
from acona_postgres_tools import acona_truncate_table, acona_data_write, acona_fetch_data, acona_fetch_one
from acona_beautifulsoup_tools import acona_parse_url, acona_parse_internal_links, acona_parse_external_links

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
tags=['etl', 'pavevariables'],
schedule_interval='30 5 * * 0')

def acona_pagevariables():

    # [END instantiate_dag]

    # [START notify]
    @task()
    def pv_extract(domain):
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

            result = acona_parse_url(url)
            soup = result[1]

            # page title
            title = ''
            title = soup.title.string

            table = 'api.var_page_title'
            data = {'value': [title], 'datetime': [date], 'url': [url]}
            dataf = pd.DataFrame(data)
            acona_data_write(table, dataf)

            #page title characters count
            title_len = 0
            title_len = len(title)

            table = 'api.var_page_title_char_count'
            data = {'value': [title_len], 'datetime': [date], 'url': [url]}
            dataf = pd.DataFrame(data)
            acona_data_write(table, dataf)

            #metatags
            metadescription = ''
            for meta in soup.find_all('meta'):
                #meta description
                if meta.get('name') == 'description':
                    metadescription = meta.get('content')

            table = 'api.var_page_metadescription'
            data = {'value': [metadescription], 'datetime': [date], 'url': [url]}
            dataf = pd.DataFrame(data)
            acona_data_write(table, dataf)

            # h1
            h1s = soup.find_all('h1')
            h1s_count = 0
            h1s_count = len(h1s)
            h1 = ''
            h1 = h1s[0].text.strip()

            table = 'api.var_page_h1'
            data = {'value': [h1], 'datetime': [date], 'url': [url]}
            dataf = pd.DataFrame(data)
            acona_data_write(table, dataf)

            table = 'api.var_page_h1_count'
            data = {'value': [h1s_count], 'datetime': [date], 'url': [url]}
            dataf = pd.DataFrame(data)
            acona_data_write(table, dataf)

            # internal links
            internal_links = acona_parse_internal_links(soup, url)
            internal_links_count = 0
            internal_links_count = len(internal_links)

            table = 'api.var_page_links_internal_count'
            data = {'value': [internal_links_count], 'datetime': [date], 'url': [url]}
            dataf = pd.DataFrame(data)
            acona_data_write(table, dataf)

            # internal links
            external_links = acona_parse_external_links(soup, url)
            external_links_count = 0
            external_links_count = len(external_links)

            table = 'api.var_page_links_external_count'
            data = {'value': [external_links_count], 'datetime': [date], 'url': [url]}
            dataf = pd.DataFrame(data)
            acona_data_write(table, dataf)

            # images
            imgs = soup.find_all('img')
            imgs_without_alt = 0
            imgs_without_size = 0
            imgs_count = 0
            imgs_count = len(imgs)
            for img in imgs:
                if img.get('alt') == None or img.get('alt') == '':
                    imgs_without_alt = imgs_without_alt + 1
                if img.get('height') == None or img.get('height') == '' or img.get('width') == None or img.get('width') == '':
                    imgs_without_size = imgs_without_size + 1

            table = 'api.var_page_images_count'
            data = {'value': [imgs_count], 'datetime': [date], 'url': [url]}
            dataf = pd.DataFrame(data)
            acona_data_write(table, dataf)

            table = 'api.var_page_images_without_alt_count'
            data = {'value': [imgs_without_alt], 'datetime': [date], 'url': [url]}
            dataf = pd.DataFrame(data)
            acona_data_write(table, dataf)

            table = 'api.var_page_images_without_size_count'
            data = {'value': [imgs_without_size], 'datetime': [date], 'url': [url]}
            dataf = pd.DataFrame(data)
            acona_data_write(table, dataf)

            # write import dates
            data = {'variable': ['pagevariables'], 'date': [date], 'url': [url]}
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
            pv_extract(domain)

    # [END main_flow]

# [START dag_invocation]
acona_pagevariables = acona_pagevariables()
# [END dag_invocation]
