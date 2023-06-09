FROM apache/airflow:2.3.3-python3.8

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
COPY ./requirements.txt /
RUN pip install -r /requirements.txt