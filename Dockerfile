FROM apache/airflow:3.3.0

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install --no-cache-dir pyspark==3.5.0 psycopg2-binary minio