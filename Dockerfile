FROM python:3.12-slim

# Java é requisito do PySpark
RUN apt-get update && \
	apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
	rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
	PYTHONPATH=/app/src \
	PYSPARK_PYTHON=python \
	PYSPARK_DRIVER_PYTHON=python \
	PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY scripts/ ./scripts/

CMD ["python", "src/run.py"]
