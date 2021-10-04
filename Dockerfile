FROM apache/airflow:2.1.4
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir --user apache-airflow-providers-postgres
RUN pip install --no-cache-dir --user pandas
RUN pip uninstall --no-cache-dir pystan
#RUN pip install --no-cache-dir --user pystan==2.18.0.0
#RUN pip install --no-cache-dir --user fbprophet
RUN pip install --no-cache-dir --user matplotlib
RUN pip install --no-cache-dir --user numpy