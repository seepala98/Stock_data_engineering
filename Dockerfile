FROM apache/airflow:2.6.0

# Install Python packages
COPY requirements.txt .
RUN pip install --user -r requirements.txt

USER airflow

COPY ./requirements.txt /
RUN pip install -r /requirements.txt

COPY --chown=airflow:airflow ./dags /opt/airflow/dags