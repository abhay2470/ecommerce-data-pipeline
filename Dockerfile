FROM apache/airflow:2.7.0

# Switch to root to install system packages
USER root

# Install OpenJDK (Java) required by PySpark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the default airflow user
USER airflow

# Install pyspark via pip
RUN pip install --no-cache-dir pyspark