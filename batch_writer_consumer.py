import json
from kafka import KafkaConsumer
import pandas as pd
import os

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = 'food-data-topic'
BATCH_SIZE = 2000 # Increased batch size for consumer
OUTPUT_DIR = 'data/batches'
CONSUMER_GROUP_ID = 'batch-writer-group'

# Must match features sent by producer (including 'description')
EXPECTED_FEATURES = [
    "Protein-G", "Total lipid (fat)-G", "Carbohydrate, by difference-G", "Energy-KCAL",
    "Sugars, total including NLEA-G", "Fiber, total dietary-G", "Calcium, Ca-MG", "Iron, Fe-MG",
    "Sodium, Na-MG", "Vitamin D (D2 + D3)-UG", "Cholesterol-MG", "Fatty acids, total saturated-G",
    "Potassium, K-MG", "Vitamin C, total ascorbic acid-MG", "Vitamin B-6-MG",
    "Vitamin B-12-UG", "Zinc, Zn-MG", "description"
]
FOOD_DESCRIPTION_COL = "description"

def create_consumer():
    # (Same as before)
    print(f"Connecting to Kafka broker at {KAFKA_BROKER} for topic {KAFKA_TOPIC}...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            group_id=CONSUMER_GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=90000 # Increased timeout
        )
        print("Successfully connected to Kafka consumer.")
        return consumer
    except Exception as e:
        print(f"Error connecting to Kafka consumer: {e}")
        return None

def consume_and_write_batches(consumer):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    batch_data = []
    batch_count = 0
    messages_processed = 0

    print(f"Starting to consume messages. Batch size: {BATCH_SIZE}")
    try:
        for message_idx, message in enumerate(consumer):
            data = message.value
            processed_data = {}
            for key in EXPECTED_FEATURES:
                if key == FOOD_DESCRIPTION_COL:
                    processed_data[key] = str(data.get(key, "Unknown Food"))
                else: # Numeric features
                    try:
                        processed_data[key] = float(data.get(key, 0.0))
                    except (ValueError, TypeError): # Handle if data.get() returns something not floatable
                        processed_data[key] = 0.0
            
            batch_data.append(processed_data)
            messages_processed += 1

            if len(batch_data) >= BATCH_SIZE:
                df_batch = pd.DataFrame(batch_data, columns=EXPECTED_FEATURES) # Enforce column order
                file_path = os.path.join(OUTPUT_DIR, f'batch_{batch_count}.csv')
                df_batch.to_csv(file_path, index=False)
                print(f"Written batch {batch_count} to {file_path} ({len(df_batch)} records)")
                batch_data = []
                batch_count += 1
            
            if messages_processed % 500 == 0:
                print(f"Processed {messages_processed} messages...")

    except Exception as e:
        print(f"Error during consumption: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if batch_data:
            df_batch = pd.DataFrame(batch_data, columns=EXPECTED_FEATURES)
            file_path = os.path.join(OUTPUT_DIR, f'batch_{batch_count}.csv')
            df_batch.to_csv(file_path, index=False)
            print(f"Written final batch {batch_count} to {file_path} ({len(df_batch)} records)")
        
        print(f"Total messages processed by batch writer: {messages_processed}")
        if consumer:
            consumer.close()
        print("Batch writer consumer closed.")

if __name__ == "__main__":
    # (Same as before)
    consumer = create_consumer()
    if consumer:
        consume_and_write_batches(consumer)