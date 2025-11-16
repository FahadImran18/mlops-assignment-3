# Dockerfile for MLOps Assignment 3 - NASA APOD ETL Pipeline
# Based on Astronomer's Apache Airflow image with additional tools

FROM apache/airflow:2.8.0

# Switch to root to install system packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Install DVC
RUN pip install --no-cache-dir dvc[s3] dvc[gdrive] dvc[gs] dvc[azure] dvc[oss] dvc[ssh]

# Create necessary directories
RUN mkdir -p /opt/airflow/data \
    && mkdir -p /opt/airflow/dvc_repo \
    && mkdir -p /opt/airflow/plugins

# Copy DAGs and plugins
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/

# Set working directory
WORKDIR /opt/airflow

# Initialize Git in DVC repo directory (will be properly initialized at runtime)
RUN cd /opt/airflow/dvc_repo && git init || true

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True

