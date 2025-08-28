FROM apache/airflow:2.0.2
USER root
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
USER airflow
