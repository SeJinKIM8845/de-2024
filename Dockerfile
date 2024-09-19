FROM apache/airflow:2.7.1-python3.11

# Switch to root to install system packages
USER root

# Install necessary packages including Java and Docker dependencies
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk wget && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Get Docker group ID from host system (GID 998 in this case) and add it to the container
RUN groupadd -g 998 docker && usermod -aG docker airflow

# Switch back to the airflow user
USER airflow

# Install Python dependencies
RUN pip install apache-airflow==2.7.1 apache-airflow-providers-apache-spark pyspark elasticsearch numpy requests
