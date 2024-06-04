FROM apache/airflow:2.7.1-python3.11

USER root

# Update package list and install required packages
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Switch to airflow user
USER airflow

# Install additional Python packages
RUN pip install apache-airflow-providers-apache-spark pyspark
