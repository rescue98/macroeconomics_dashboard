FROM apache/airflow:2.8.1-python3.10

USER root

# Install Java for Spark and other system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Install Python packages for ETL
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy ETL application code only
COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./spark_jobs /opt/airflow/spark_jobs

# Create necessary directories
RUN mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/plugins

# Set working directory
WORKDIR /opt/airflow