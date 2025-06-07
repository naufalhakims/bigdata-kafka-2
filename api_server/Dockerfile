FROM python:3.9-slim

WORKDIR /app

COPY requirements_api.txt .
RUN pip install --no-cache-dir -r requirements_api.txt

# Install Java for PySpark (needed even for loading models and running Spark locally in API)
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

COPY api.py .
# Models will be mounted via docker-compose volume

EXPOSE 5000

CMD ["python", "api.py"]