FROM apache/airflow:2.8.1-python3.9

USER root
RUN apt-get update && apt-get install -y git curl

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Install dbt
RUN pip install dbt-postgres==1.7.18

# Create necessary directories
RUN mkdir -p /opt/airflow/dbt /opt/airflow/monitoring

WORKDIR /opt/airflow
