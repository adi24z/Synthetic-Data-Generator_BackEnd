FROM apache/airflow:2.9.1

USER root

# Install Java + curl
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl && \
    apt-get clean

# Correct JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Copy requirements
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt