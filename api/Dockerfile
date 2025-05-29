# Gunakan base image Spark dari Bitnami yang sama
FROM bitnami/spark:3.5-debian-11

# USER root # Opsional, sama seperti di atas.

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 5000
# Override CMD untuk menjalankan aplikasi Flask Anda
CMD ["python", "app.py"]