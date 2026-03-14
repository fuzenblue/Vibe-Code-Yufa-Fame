FROM apache/airflow:2.9.3-python3.11

USER root

# Install system dependencies for MySQL client
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-libmysqlclient-dev \
        build-essential \
        pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
