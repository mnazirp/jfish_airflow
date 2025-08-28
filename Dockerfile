FROM apache/airflow:2.7.3-python3.9

# Copy requirements.txt
COPY requirements.txt /requirements.txt

# Install packages as airflow user (default user di image Airflow)
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
