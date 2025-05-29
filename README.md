# Proyek Deteksi Alergen Makanan dengan Kafka dan Spark

Proyek ini mensimulasikan sistem Big Data untuk pemrosesan data makanan secara streaming menggunakan Apache Kafka dan Apache Spark. Tujuan utamanya adalah memungkinkan pengguna untuk mencari makanan berdasarkan alergen yang dikandungnya melalui sebuah API.

**Teknologi yang Digunakan:**
* Docker & Docker Compose
* Apache Kafka (untuk data streaming)
* Apache Spark (untuk pemrosesan data dan pembuatan "model" pencarian)
* Python
    * `kafka-python` untuk Producer dan Consumer Kafka
    * `pyspark` untuk script pemrosesan Spark
    * `Flask` untuk membuat REST API
* Dataset: Kandungan bahan pada setiap makanan (format CSV)

## Struktur Direktori Utama (Contoh)
food_allergen_project/
├── docker-compose.yml
├── data/                     # TIDAK di-commit ke Git, dibuat manual
│   └── food_data.csv         # Dataset awal Anda (ditempatkan manual)
│   └── kafka_batches/      # Dibuat oleh Kafka Consumer
│   └── processed_data/     # Dibuat oleh Spark Processor
├── kafka_producer/
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements.txt
├── kafka_consumer/
│   ├── consumer.py
│   ├── Dockerfile
│   └── requirements.txt
├── spark_processor/
│   ├── processor.py
│   ├── Dockerfile
│   └── requirements.txt
└── api/
├── app.py
├── Dockerfile
└── requirements.txt
└── README.md                 # File ini
└── .gitignore                # Mengabaikan folder data/, dll.


## Prasyarat
Sebelum memulai, pastikan Anda telah menginstal:
1.  **Docker**: [Instruksi Instalasi Docker](https://docs.docker.com/get-docker/)
2.  **Docker Compose**: Biasanya sudah terinstal bersama Docker Desktop. [Instruksi Instalasi Docker Compose](https://docs.docker.com/compose/install/)
3.  **Git**: Untuk mengkloning repository (jika Anda membagikannya).

## Pengaturan Proyek

1.  **Kloning Repository (Jika Ada):**
    ```bash
    git clone <url_repository_anda>
    cd food_allergen_project
    ```

2.  **Siapkan Dataset `food_data.csv`:**
    * Buat direktori `data/` di root proyek Anda jika belum ada:
        ```bash
        mkdir data
        ```
    * Tempatkan file dataset Anda dengan nama `food_data.csv` ke dalam direktori `food_allergen_project/data/`.
    * **PENTING**: Dataset ini **TIDAK** akan di-commit ke Git karena ukurannya yang besar (jika menggunakan `.gitignore` dengan benar). Anda perlu menyediakannya secara manual.
    * Pastikan dataset Anda memiliki header dengan kolom-kolom yang relevan, minimal: `fdc_id`, `description`, dan `ingredients`.
    * **REKOMENDASI UNTUK PENGEMBANGAN**: Jika dataset Anda sangat besar (misalnya, 2 juta baris), sangat disarankan untuk membuat **sampel data yang lebih kecil** (misalnya, 10.000 - 50.000 baris) dan menamakannya `food_data.csv` untuk digunakan selama pengembangan dan pengujian awal. Ini akan mempercepat proses build dan run secara signifikan. Anda bisa mengubah nama file di `kafka_producer/producer.py` jika menggunakan nama sampel yang berbeda.

3.  **Konfigurasi `.gitignore` (Jika Belum):**
    Pastikan file `.gitignore` di root proyek Anda berisi setidaknya baris berikut untuk mencegah folder `data` (yang berisi data mentah, batch, dan data yang diproses) terunggah ke Git:
    ```gitignore
    data/
    *.pyc
    __pycache__/
    venv/
    .DS_Store
    ```
    Tambahkan dan commit file `.gitignore` jika Anda baru membuatnya atau mengubahnya.

## Menjalankan Proyek

1.  **Bangun dan Jalankan Semua Layanan dengan Docker Compose:**
    Buka terminal di direktori root proyek (`food_allergen_project/`) dan jalankan perintah berikut:
    ```bash
    docker-compose up --build
    ```
    * `--build`: Akan memaksa Docker untuk membangun ulang image jika ada perubahan pada Dockerfile atau kode di dalam direktori layanan.
    * Perintah ini akan memulai semua layanan yang didefinisikan dalam `docker-compose.yml` sesuai urutan dependensinya.

2.  **Alur Eksekusi yang Diharapkan:**
    * **Zookeeper & Kafka**: Akan dimulai pertama kali sebagai infrastruktur messaging.
    * **Kafka Producer (`kafka_producer_app`)**: Akan membaca `food_data.csv` baris per baris, mengirimkannya ke topik Kafka, dan kemudian berhenti (exit).
        * _Catatan: Jeda random di producer mungkin telah dinonaktifkan untuk mempercepat pemuatan data awal. Tujuan aslinya adalah untuk mensimulasikan aliran data._
    * **Kafka Consumer (`kafka_consumer_app`)**: Akan membaca data dari topik Kafka, menyimpannya dalam bentuk file batch CSV di direktori `data/kafka_batches/`, dan kemudian berhenti setelah tidak ada pesan baru atau timeout.
    * **Spark Processor (`spark_processor_app`)**: Akan berjalan setelah consumer. Script ini membaca file-file batch CSV dari `data/kafka_batches/`, memprosesnya (misalnya, mengubah `ingredients` menjadi huruf kecil), dan menyimpan "model" data (dalam format Parquet) di direktori `data/processed_data/model_X/`. Setelah selesai, layanan ini juga akan berhenti.
    * **API (`food_api_app`)**: Akan dimulai dan tetap berjalan, siap menerima permintaan HTTP setelah Spark Processor selesai menyiapkan data.

3.  **Memantau Log:**
    Anda bisa melihat log gabungan dari semua layanan di terminal tempat Anda menjalankan `docker-compose up`. Untuk melihat log layanan tertentu secara spesifik, buka terminal baru dan jalankan:
    ```bash
    docker-compose logs -f <nama_layanan>
    ```
    Contoh: `docker-compose logs -f kafka_producer_app` atau `docker-compose logs -f spark_processor_app`.
    Ganti `<nama_layanan>` dengan nama layanan dari `docker-compose.yml` (misalnya `kafka_producer`, `kafka_consumer`, `spark_processor`, `api`).

4.  **Menghentikan Proyek:**
    Untuk menghentikan semua layanan dan menghapus kontainer, jaringan, dan volume yang dibuat (jika tidak didefinisikan sebagai eksternal), tekan `Ctrl+C` di terminal tempat `docker-compose up` berjalan, lalu jalankan:
    ```bash
    docker-compose down
    ```
    Jika Anda hanya ingin menghentikan tanpa menghapus, Anda bisa menggunakan `docker-compose stop`.

## Menggunakan API

Setelah semua layanan berjalan dan Spark Processor selesai memproses data, API akan siap digunakan. Secara default, API akan berjalan di `http://localhost:5000`.

Berikut adalah contoh endpoint yang tersedia:

1.  **Mencari Makanan Berdasarkan Alergen:**
    * `GET http://localhost:5000/find_allergen/model1?allergy=<nama_alergen>`
    * `GET http://localhost:5000/find_allergen/model2?allergy=<nama_alergen>`
    * `GET http://localhost:5000/find_allergen/model3?allergy=<nama_alergen>`
    * Ganti `<nama_alergen>` dengan alergen yang ingin dicari (misalnya, `milk`, `peanut`, `egg`). Model 1, 2, dan 3 merepresentasikan dataset yang diproses dari porsi data yang berbeda (kumulatif).

2.  **Melihat Detail Makanan Berdasarkan FDC ID:**
    * `GET http://localhost:5000/food_details/model1/<fdc_id>`
    * `GET http://localhost:5000/food_details/model2/<fdc_id>`
    * `GET http://localhost:5000/food_details/model3/<fdc_id>`
    * Ganti `<fdc_id>` dengan ID makanan yang valid.

3.  **Melihat Statistik Model:**
    * `GET http://localhost:5000/stats/model1`
    * `GET http://localhost:5000/stats/model2`
    * `GET http://localhost:5000/stats/model3`
    * Menampilkan jumlah total record dalam model data yang diproses.

## Tips Pengembangan

* **Menjalankan Ulang Job Tertentu**: Jika Anda mengubah kode untuk job sekali jalan seperti `spark_processor` dan ingin menjalankannya ulang tanpa memulai ulang semua layanan:
    1.  Bangun ulang image spesifik: `docker-compose build spark_processor`
    2.  Jalankan job: `docker-compose run --rm spark_processor`
* **Sumber Daya Docker**: Pastikan Docker Desktop memiliki alokasi memori dan CPU yang cukup, terutama saat Spark sedang memproses data besar. Anda bisa mengaturnya di Settings/Preferences Docker Desktop.

## Troubleshooting Umum
* **Error Koneksi Saat Build (apt-get, dll.)**: Biasanya masalah jaringan sementara atau masalah dengan mirror paket. Mencoba build ulang atau menggunakan base image yang sudah jadi (seperti `bitnami/spark` yang telah kita gunakan) dapat membantu.
* **Layanan Tidak Mulai**: Periksa log layanan dengan `docker-compose logs <nama_layanan>` untuk pesan error spesifik.
* **`data/food_data.csv` tidak ditemukan oleh Producer**: Pastikan file CSV ada di direktori `food_allergen_project/data/` dan namanya persis `food_data.csv` (sesuai path di `producer.py`).

---