from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
from acona_postgres_tools import acona_truncate_table, acona_data_write, acona_fetch_data, acona_fetch_one
import pandas as pd

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
tags=['score'],
schedule_interval='0 4 * * *')

def acona_success_scores():

    # [END instantiate_dag]

    # [START success_score]
    @task()
    def success_score():
        """
        #### Calculate success score for every url and page type
        """
        import json
        import requests
        import os
        import urllib.parse

        WAREHOUSE_TOKEN = Variable.get("WAREHOUSE_TOKEN")
        WAREHOUSE_URL = Variable.get("WAREHOUSE_URL")

        # Load domains
        domains = os.popen('curl ' + WAREHOUSE_URL + '/rpc/acona_domains -H "Authorization: Bearer ' + WAREHOUSE_TOKEN + '"').read()
        # Load variables
        variables = os.popen('curl ' + WAREHOUSE_URL + '/rpc/acona_variables -H "Authorization: Bearer ' + WAREHOUSE_TOKEN + '"').read()
        success_scores = {}

        for domain in json.loads(domains):
            domain_id = domain['domain_id']
            # Get pagetypes and success score config for domain
            pagetypes = os.popen('curl ' + WAREHOUSE_URL + '/rpc/acona_page_types ' '-d domain_id=' + domain_id + ' -H "Authorization: Bearer ' + WAREHOUSE_TOKEN + '"').read()
            pagetypes = json.loads(pagetypes)
            # Sort so we have always the same order by id.
            def get_page_type_id(pagetypes):
                return pagetypes.get('id')
            pagetypes.sort(key=get_page_type_id)
            #[{'id': 4, 'domain_id': 1, 'title': 'Landingpage'}, {'id': 22, 'domain_id': 1, 'title': 'Frontpage'}]
            success_scores[domain_id] = []
            for pagetype in pagetypes:
                page_type_id = pagetype['id']
                #Load success score config for pagetype
                config = os.popen('curl ' + WAREHOUSE_URL + '/rpc/acona_success_score_defs ' '-d page_type_id=' + str(page_type_id) + ' -H "Authorization: Bearer ' + WAREHOUSE_TOKEN + '"').read()
                success_scores[domain_id].append({pagetype['id']: json.loads(config)})

            # Load urls for domain
            urls = os.popen('curl ' + WAREHOUSE_URL + '/rpc/acona_urls_by_domain_id -d domain_id=' + domain_id +' -H "Authorization: Bearer ' + WAREHOUSE_TOKEN + '"').read()
            #Loop through urls and calculate score
            for url in json.loads(urls):
                #{'url': 'https://www.acona.app/about', 'domain_id': '1'}
                url_domain_id = url['domain_id']
                url_success_scores = success_scores.get(url_domain_id)
                #return success_scores
                for index, item in enumerate(url_success_scores):
                  url_page_type_id = index + 1
                  for key in item:
                    url_variables = item[key]
                    score = 0
                    for url_var in url_variables:
                      #Load variable table name
                      url_var_id = url_var['variable_id']
                      relevance = url_var['relevance']
                      type = url_var['type']
                      vars = json.loads(variables)
                      url_var_table = vars[url_var_id]['var_data_table']
                      url_var_table = 'metric_d_page_views'
                      #Load LAST value for variable and url
                      if url_var_table:
                        try:
                          sql = "select value from api." + url_var_table + " where url = '{}' ORDER BY date DESC LIMIT 1".format(url['url'])
                          values = acona_fetch_one(sql)
                          value = values[0]
                        except:
                          value = 0
                        value_score = 0
                        norm_value = 0

                        # Normalization based on value type
                        """
                        #### Var types:
                        100000|Number (max 100.000)
                        10000|Number (max 10.000)
                        1000|Number (max 1.000)
                        100|Number (max 100)
                        10|Number (max 10)
                        1|Number (max 1)
                        percent|Percent
                        boolean|Boolean
                        """
                        if type == 'percent':
                          norm_value = value
                        elif type == 'boolean':
                          if value == '1':
                            norm_value = 100
                          else:
                            norm_value = 0
                        else:
                            norm_value = value / int(type)

                        # Priority
                        value_score = norm_value * int(relevance)
                        score = score + value_score

                # save score for yesterday
                date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
                data = {'url': [url['url']], 'date': [date], 'value': [score]}
                dataf = pd.DataFrame(data)
                # save general score
                acona_data_write('api.metric_success_score_ratio', dataf)
                # save also in page type id specific table
                acona_data_write('api.metric_success_score_type' + str(url_page_type_id) + '_ratio', dataf)

                # write calc dates
                data = {'variable': ['api.metric_success_score_ratio'], 'date': [date], 'url': [url['url']]}
                dataf = pd.DataFrame(data)
                acona_data_write('internal.var_calc_dates', dataf)

                data = {'variable': ['api.metric_success_score_type' + str(url_page_type_id) + '_ratio'], 'date': [date], 'url': [url['url']]}
                dataf = pd.DataFrame(data)
                acona_data_write('internal.var_calc_dates', dataf)

    # [END success_score]

    # [START main_flow]

    success_score()

    # [END main_flow]

# [START dag_invocation]
acona_success_scores = acona_success_scores()
# [END dag_invocation]
