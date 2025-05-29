import json
import os
import time
from kafka import KafkaConsumer
import pandas as pd

KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'food_ingredients_topic'
GROUP_ID = 'food_consumer_group'
BATCH_SIZE = 1000 # Simpan setiap 1000 pesan
OUTPUT_DIR = '/app/data/kafka_batches/' # Path di dalam container

def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest', # Mulai baca dari awal jika consumer baru
                enable_auto_commit=True,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=10000 # Timeout setelah 10 detik jika tidak ada pesan baru
            )
            print("Kafka Consumer connected successfully!")
            return consumer
        except Exception as e:
            print(f"Error connecting to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def main():
    consumer = create_consumer()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    messages_batch = []
    batch_count = 0
    message_counter = 0

    print(f"Listening for messages on topic: {KAFKA_TOPIC}")
    print(f"Saving batches to: {OUTPUT_DIR}")

    try:
        for message in consumer:
            messages_batch.append(message.value)
            message_counter += 1
            print(f"Received message {message_counter}: {message.value.get('description', 'N/A Food')[:50]}")

            if len(messages_batch) >= BATCH_SIZE:
                batch_count += 1
                file_path = os.path.join(OUTPUT_DIR, f'batch_{batch_count}.csv')
                df = pd.DataFrame(messages_batch)
                df.to_csv(file_path, index=False)
                print(f"Saved batch {batch_count} with {len(messages_batch)} messages to {file_path}")
                messages_batch = []

    except Exception as e:
        print(f"An error occurred while consuming messages: {e}")
    finally:
        # Simpan sisa pesan jika ada
        if messages_batch:
            batch_count += 1
            file_path = os.path.join(OUTPUT_DIR, f'batch_{batch_count}_final.csv')
            df = pd.DataFrame(messages_batch)
            df.to_csv(file_path, index=False)
            print(f"Saved final batch {batch_count} with {len(messages_batch)} messages to {file_path}")
        consumer.close()
        print("Kafka Consumer closed.")

if __name__ == '__main__':
    # Tunggu Kafka siap
    time.sleep(20)
    main()