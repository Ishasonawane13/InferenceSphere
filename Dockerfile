FROM python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYSPARK_SUBMIT_ARGS="--repositories https://repo.maven.apache.org/maven2 pyspark-shell"

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY scripts ./scripts
COPY src ./src
COPY config.yaml .

CMD ["python", "scripts/smoke_test_graphframes.py"]
