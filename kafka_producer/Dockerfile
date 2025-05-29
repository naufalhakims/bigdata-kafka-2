FROM python:3.9-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Dataset akan di-mount melalui volume di docker-compose.yml
# Jadi tidak perlu di-COPY ke Dockerfile ini secara spesifik
# Tapi pastikan path di producer.py sesuai dengan mount point.

CMD ["python", "producer.py"]