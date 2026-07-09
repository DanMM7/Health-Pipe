FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY docker/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt