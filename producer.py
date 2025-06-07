import csv
from kafka import KafkaProducer
import json
import time
import random
import os

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = 'food-data-topic'
DATASET_PATH = 'food.csv'

# Master list of columns needed for ANY model
SELECTED_FEATURES_ORDERED = [
    "Protein-G", "Total lipid (fat)-G", "Carbohydrate, by difference-G", "Energy-KCAL",
    "Sugars, total including NLEA-G", "Fiber, total dietary-G", "Calcium, Ca-MG", "Iron, Fe-MG",
    "Sodium, Na-MG", "Vitamin D (D2 + D3)-UG", "Cholesterol-MG", "Fatty acids, total saturated-G",
    "Potassium, K-MG", "Vitamin C, total ascorbic acid-MG", "Vitamin B-6-MG",
    "Vitamin B-12-UG", "Zinc, Zn-MG", "description" # Ensure this is the correct name
]
FOOD_DESCRIPTION_COL = "description" # For special handling

def create_producer():
    # (Same as before)
    print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=10,
            batch_size=16384 * 2
        )
        print("Successfully connected to Kafka broker.")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

def send_data_with_csv_module(producer, topic):
    records_sent_count = 0
    bad_lines_count = 0
    try:
        print(f"Reading dataset from {DATASET_PATH} using csv module...")
        with open(DATASET_PATH, 'r', encoding='utf-8', errors='replace') as csvfile:
            reader_for_header = csv.reader(csvfile)
            try:
                header = next(reader_for_header)
            except StopIteration:
                print("Error: CSV file is empty or has no header.")
                return
            
            print(f"CSV Header: {header}")

            feature_to_index = {}
            missing_selected_cols_in_header = []
            for feature_name in SELECTED_FEATURES_ORDERED:
                try:
                    feature_to_index[feature_name] = header.index(feature_name)
                except ValueError:
                    missing_selected_cols_in_header.append(feature_name)
            
            if missing_selected_cols_in_header:
                print(f"WARNING: The following SELECTED_FEATURES were NOT found in the CSV header: {missing_selected_cols_in_header}")
                current_selected_features = [f for f in SELECTED_FEATURES_ORDERED if f in feature_to_index]
                if not current_selected_features:
                    print("CRITICAL: None of the selected features were found in the CSV header. Exiting.")
                    return
            else:
                current_selected_features = SELECTED_FEATURES_ORDERED
                print("All selected features found in CSV header.")

            print(f"Starting to send data to Kafka topic: {topic}")
            for row_num, row_list in enumerate(reader_for_header):
                message = {}
                try:
                    for feature_name in current_selected_features:
                        col_index = feature_to_index[feature_name]
                        if col_index < len(row_list):
                            raw_value = row_list[col_index]
                            if feature_name == FOOD_DESCRIPTION_COL:
                                message[feature_name] = str(raw_value) # Keep description as string
                            else: # Attempt to convert other features to float
                                try:
                                    message[feature_name] = float(raw_value)
                                except (ValueError, TypeError):
                                    message[feature_name] = 0.0 # Default for non-numeric or empty
                        else:
                            message[feature_name] = 0.0 if feature_name != FOOD_DESCRIPTION_COL else "Unknown"
                    
                    producer.send(topic, value=message)
                    records_sent_count += 1

                    if records_sent_count % 2000 == 0: # Increased batch printout
                        print(f"Sent {records_sent_count} records...")
                        producer.flush()

                except IndexError: # Row shorter than expected by header mapping
                    bad_lines_count += 1
                except Exception as e_row:
                    bad_lines_count += 1
                    # print(f"Error processing row {row_num + 1}: {e_row}. Row: {row_list[:10]}") # Print only first few elements

        producer.flush()
        print(f"All data processed. Total records sent to Kafka: {records_sent_count}")
        if bad_lines_count > 0:
            print(f"Number of bad/skipped lines during row processing: {bad_lines_count}")

    except FileNotFoundError:
        print(f"Error: Dataset file not found at {DATASET_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # (Same as before)
    if not os.path.exists(DATASET_PATH):
        print(f"CRITICAL ERROR: Dataset file '{DATASET_PATH}' not found.")
    else:
        kafka_producer = create_producer()
        if kafka_producer:
            os.makedirs("data/batches", exist_ok=True)
            os.makedirs("data/models", exist_ok=True)
            send_data_with_csv_module(kafka_producer, KAFKA_TOPIC)
            kafka_producer.close()
            print("Producer closed.")
        else:
            print("Could not create Kafka producer. Exiting.")