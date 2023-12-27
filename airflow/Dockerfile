FROM apache/airflow:2.8.0
USER root
RUN mkdir -p /opt/airflow/data /opt/airflow/dataset /opt/airflow/staging_area && \
    chown -R airflow: /opt/airflow/data /opt/airflow/dataset /opt/airflow/staging_area
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==2.8.0" -r /requirements.txt
