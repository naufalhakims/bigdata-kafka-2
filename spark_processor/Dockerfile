# Gunakan base image Spark dari Bitnami yang sudah termasuk Python
# Pilih versi Spark dan Debian yang sesuai.
# Image ini biasanya sudah memiliki Java, Spark, Python, dan Hadoop client.
FROM bitnami/spark:3.5-debian-11

# USER root # Opsional: Beberapa image Bitnami berjalan sebagai non-root.
            # Jika Anda perlu melakukan operasi yang memerlukan root, uncomment baris ini,
            # lakukan operasi tersebut, lalu bisa kembali ke user non-root jika perlu.
            # Untuk 'pip install --user' atau jika direktori /app writable, root mungkin tidak perlu.

WORKDIR /app

# Salin file requirements.txt Anda
COPY ./requirements.txt /app/requirements.txt

# Install Python dependencies dari requirements.txt
# Bitnami Spark image biasanya sudah memiliki pip.
RUN pip install --no-cache-dir -r requirements.txt

# Salin sisa kode aplikasi Anda ke direktori kerja
COPY . /app

# CMD default dari image bitnami/spark mungkin adalah skrip untuk menjalankan Spark.
# Kita override untuk menjalankan skrip Python kita.
# PYSPARK_PYTHON dan SPARK_HOME biasanya sudah dikonfigurasi oleh base image Bitnami.
CMD ["python", "processor.py"]