FROM apache/airflow:2.1.4
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         libc-dev \
         python3-dev \
         libevent-dev \
         libzbar-dev \
         build-essential
RUN apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir --user apache-airflow-providers-postgres
RUN pip install pip
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install --no-cache-dir --user ipython==7.5.0
RUN pip install --no-cache-dir --user pandas
RUN pip install --no-cache-dir --user pystan==2.19.1.1 prophet
RUN pip install --no-cache-dir --user matplotlib
RUN pip install --no-cache-dir --user numpy