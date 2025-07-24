FROM apache/airflow:2.8.1-python3.10

USER root

# Install Java 17 for Spark and other system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install only essential packages to avoid conflicts
RUN pip install --no-cache-dir \
    pandas==1.5.3 \
    numpy==1.24.3 \
    psycopg2-binary==2.9.7 \
    minio==7.1.16 \
    requests==2.31.0 \
    sqlalchemy==1.4.39 \
    pendulum==2.1.2

# Copy ETL application code only
COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./spark_jobs /opt/airflow/spark_jobs

# Create necessary directories
RUN mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/plugins

# Set working directory
WORKDIR /opt/airflow