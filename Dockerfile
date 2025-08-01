FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    pandas==1.5.3 \
    numpy==1.24.3 \
    psycopg2-binary==2.9.7 \
    minio==7.1.16 \
    requests==2.31.0 \
    sqlalchemy==1.4.39 \
    pendulum==2.1.2 \
    scikit-learn==1.3.0 \
    joblib==1.3.2 \
    xgboost==1.7.6

COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./spark_jobs /opt/airflow/spark_jobs

RUN mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/plugins

WORKDIR /opt/airflow