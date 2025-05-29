import csv
import json
import time
import random
from kafka import KafkaProducer

KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'food_ingredients_topic'
DATASET_PATH = '/app/data/food_data.csv' # Path di dalam container

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1) # Sesuaikan jika perlu
            )
            print("Kafka Producer connected successfully!")
            return producer
        except Exception as e:
            print(f"Error connecting to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def main():
    producer = create_producer()
    header = []

    print(f"Reading dataset from: {DATASET_PATH}")
    try:
        with open(DATASET_PATH, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader) # Baca header

            for i, row in enumerate(csv_reader):
                message = dict(zip(header, row))
                producer.send(KAFKA_TOPIC, value=message)
                print(f"Sent: {message.get('description', 'N/A Food')[:50]}")

                # # Jeda random antara 0.1 hingga 1 detik
                # sleep_time = random.uniform(0.1, 1.0)
                # time.sleep(sleep_time)

                if (i + 1) % 100 == 0: # Log setiap 100 baris
                    print(f"Sent {i+1} records.")

    except FileNotFoundError:
        print(f"ERROR: Dataset file not found at {DATASET_PATH}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.flush()
        producer.close()
        print("Kafka Producer closed.")

if __name__ == '__main__':
    # Tunggu Kafka siap (opsional, tapi bagus untuk docker-compose)
    time.sleep(20)
    main()