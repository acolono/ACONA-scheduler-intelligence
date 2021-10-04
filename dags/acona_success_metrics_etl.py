
import json
import requests
import os
import urllib.parse

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from datetime import datetime, timedelta

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
tags=['matomo', 'gsc'],
schedule_interval="@daily")

def acona_success_metrics_etl():

    # [END instantiate_dag]

    # [START extract]
    @task()
    def extract():
        """
        #### Get data from configured services
        """
        #ARGUMENTS

        #GENERAL
        #DATE = start_date + ',' + end_date
        DATE = 'previous1' #yesterday
        #TIME = int(round(time.time() * 1000))

        WAREHOUSE_TOKEN = Variable.get("WAREHOUSE_TOKEN")
        WAREHOUSE_URL = Variable.get("WAREHOUSE_URL")

        urls = os.popen('curl ' + WAREHOUSE_URL + '/rpc/acona_urls -H "Authorization: Bearer ' + WAREHOUSE_TOKEN + '"').read()

        #Prepare metrics dictionary.
        day = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        matomo_values = {}
        gsc_values = {}

        #START MATOMO
        MATOMO_HEADERS = {
          'Content-Type': 'application/json; charset=utf-8'
        }
        for url in json.loads(urls):
          #Load matomo instance url by domain
          DOMAIN_ID = url.get('domain_id')
          MATOMO_URL = Variable.get("MATOMO_URL_" + DOMAIN_ID) #Without trailing slash
          #Load matomo auth token by domain
          TOKEN_AUTH = Variable.get("MATOMO_TOKEN_" + DOMAIN_ID)
          URL = urllib.parse.quote(url.get('url'), safe='')
          request_url = MATOMO_URL + '/?module=API&method=API.getBulkRequest&format=json&token_auth=' + TOKEN_AUTH + '&urls[0]=method%3dVisitsSummary.get%26idSite%3d6%26date%3d' + DATE + '%26period%3dday&segment=pageUrl==' + URL

          r = requests.get(request_url, headers=MATOMO_HEADERS)
          url_values = r.json()
          #Update metrics dictionary with matomo results.
          matomo_values.update({url.get('url'): url_values[0].get(day)})
        #END MATOMO

        metrics = {'date': day, 'matomo': matomo_values, 'gsc': gsc_values}
        return metrics

    # [END extract]

    # [START transform]
    @task()
    def transform(metrics: dict):
        """
        #### Transform task
        Prepare data structure.
        #Required format:
                {'metric_d_visits': [
                        {"url": "https://www.acolono.com", "value": "10", "date": "2021-09-19"},
                        {"url": "https://www.acolono.com/about", "value": "10", "date": "2021-09-19"},
                        {"url": "https://www.acolono.com/metrics", "value": "11", "date": "2021-09-19"}
                        ],
                'metric_d_page_views': [
                        {"url": "https://www.acolono.com", "value": "10", "date": "2021-09-19"},
                        {"url": "https://www.acolono.com/about", "value": "10", "date": "2021-09-19"},
                        {"url": "https://www.acolono.com/metrics", "value": "11", "date": "2021-09-19"}
                        ]
                }
        """

        #metrics = {'date': '2021-10-01', 'matomo': {'https://www.acona.app/about': [], 'https://www.acona.app/metrics': [], 'https://www.acona.app/legal': [], 'https://www.acona.app/info': [], 'https://www.acona.app/': [], 'https://www.acolono.com/about': [], 'https://www.acolono/metrics': [], 'https://www.acolono.com/produkte': [], 'https://www.acolono.com/betterembed': [], 'https://www.acolono.com/': {'nb_uniq_visitors': 1, 'nb_users': 0, 'nb_visits': 1, 'nb_actions': 2, 'nb_visits_converted': 1, 'bounce_count': 0, 'sum_visit_length': 21, 'max_actions': 2, 'bounce_rate': '0%', 'nb_actions_per_visit': 2, 'avg_time_on_site': 21}, 'https://www.acolono.com/jobs': [], 'https://www.acolono.com/en/blog/drupal-kubernetes-getting-started-insights': []}, 'gsc': {}}

        datatables = {
          'nb_visits': 'metric_d_visits',
          'nb_actions': 'metric_d_page_views',
          'nb_visits_converted': 'metric_d_conversions',
          'nb_uniq_visitors': 'metric_d_unique_visits',
          'bounce_count': 'metric_d_bounces',
          'sum_visit_length': 'metric_d_visit_time_total',
          'avg_time_on_site': 'metric_d_visit_time_average',
          'nb_visits_converted': 'metric_d_visits_converted',
          'bounce_rate': 'metric_d_bounce_rate'
        }

        # Create a dictionary for matomo data.
        matomo_data = {}
        # Inside the dictionary we want an array of values for every table.
        for key in datatables:
          matomo_data[datatables[key]] = []

        matomo = metrics.get('matomo')
        date = metrics.get('date')
        for key in matomo:
          url = key
          values = matomo[key]
          for key in datatables:
            if values:
              values = values
              matomo_data[datatables[key]].append({"url": url, "value": values[key], "date": date})
            else:
              matomo_data[datatables[key]].append({"url": url, "value": 0, "date": date})

        data = {'matomo': matomo_data}
        return data

    # [END transform]

    # [START load]
    @task()
    def load(metrics: dict):
        """
        #### Load task
        Save metrics in the data warehouse in the required format. Prepare curl command to send it to BashOperator task.
        """

        WAREHOUSE_TOKEN = Variable.get("WAREHOUSE_TOKEN")
        WAREHOUSE_URL = Variable.get("WAREHOUSE_URL")
        command = ''

        #START MATOMO
        matomo_data = metrics.get('matomo')
        for key in matomo_data:
          #data = [{"url": "https://www.acolono.com", "value": "10", "date": "2021-09-19"}, {"url": "https://www.acolono.com", "value": "111", "date": "2021-09-20"}]
          table = key
          data = matomo_data[key]
          command = command + 'curl ' + WAREHOUSE_URL + '/' +  table + ' -X POST -H "Authorization: Bearer ' + WAREHOUSE_TOKEN +'" -H "Content-Type: application/json" -d \'' + json.dumps(data, ensure_ascii=False) + '\';'
        #END MATOMO
        return command

    # [END load]

    # [START main_flow]
    metrics = extract()
    metrics_transformed = transform(metrics)
    command = load(metrics_transformed)

    BashOperator(task_id='bash_task', bash_command=command, do_xcom_push=True)

    # [END main_flow]


# [START dag_invocation]
acona_success_metrics_etl = acona_success_metrics_etl()
# [END dag_invocation]

# [END tutorial]