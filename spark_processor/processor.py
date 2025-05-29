from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when
import os
import glob

INPUT_DIR = '/app/data/kafka_batches/' # Path batch CSV di container
OUTPUT_DIR = '/app/data/processed_data/' # Path output Parquet di container
RELEVANT_COLUMNS = ["fdc_id", "description", "ingredients"]

def main():
    spark = SparkSession.builder \
        .appName("FoodAllergenProcessor") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Temukan semua file batch CSV
    batch_files = sorted(glob.glob(os.path.join(INPUT_DIR, 'batch_*.csv')))

    if not batch_files:
        print(f"No batch files found in {INPUT_DIR}. Exiting.")
        spark.stop()
        return

    print(f"Found batch files: {batch_files}")

    # Tentukan jumlah total baris untuk pembagian data
    # Atau, kita bisa mendefinisikan model berdasarkan jumlah file batch
    # Misal, jika ada N file batch, kita bisa buat N model kumulatif
    # Atau, tetap pada 3 model seperti contoh di soal

    num_total_batches = len(batch_files)
    if num_total_batches == 0:
        print("No batch files found.")
        spark.stop()
        return

    # Skema Model B: kumulatif
    # Model 1: batch pertama
    # Model 2: batch pertama + kedua
    # Model 3: batch pertama + kedua + ketiga (atau semua jika kurang dari 3)

    processed_dfs = [] # Untuk menyimpan DataFrame yang sudah dibaca agar tidak dibaca ulang

    for i in range(num_total_batches):
        current_batch_file = batch_files[i]
        print(f"Processing {current_batch_file} for model building...")
        try:
            # Baca batch file baru
            # Pastikan Spark bisa infer schema atau definisikan secara eksplisit jika perlu
            new_df = spark.read.option("header", "true").option("inferSchema", "true").csv(current_batch_file)
            
            # Pilih kolom yang relevan
            new_df_selected = new_df.select(RELEVANT_COLUMNS)
            
            # Preprocessing: lowercase ingredients, handle nulls
            new_df_processed = new_df_selected.withColumn(
                "ingredients_processed",
                lower(when(col("ingredients").isNotNull(), col("ingredients")).otherwise(""))
            )
            processed_dfs.append(new_df_processed)

            # Gabungkan dengan DataFrame sebelumnya untuk model kumulatif
            if i == 0: # Model 1
                cumulative_df_model1 = processed_dfs[0]
                model1_path = os.path.join(OUTPUT_DIR, 'model_1')
                print(f"Saving Model 1 (from {batch_files[0]}) to {model1_path}")
                cumulative_df_model1.write.mode('overwrite').parquet(model1_path)
            
            if i == 1: # Model 2
                cumulative_df_model2 = processed_dfs[0].unionByName(processed_dfs[1])
                model2_path = os.path.join(OUTPUT_DIR, 'model_2')
                print(f"Saving Model 2 (from {', '.join(batch_files[:2])}) to {model2_path}")
                cumulative_df_model2.write.mode('overwrite').parquet(model2_path)

            if i == 2 or (i == num_total_batches -1 and i < 2) : # Model 3 (atau model terakhir jika kurang dari 3 batch)
                # Buat DataFrame kumulatif dari semua DFS yang sudah diproses sejauh ini
                final_cumulative_df = processed_dfs[0]
                for df_idx in range(1, len(processed_dfs)):
                    final_cumulative_df = final_cumulative_df.unionByName(processed_dfs[df_idx])
                
                model3_path = os.path.join(OUTPUT_DIR, 'model_3')
                source_files_str = ', '.join(batch_files[:len(processed_dfs)])
                print(f"Saving Model 3 (from {source_files_str}) to {model3_path}")
                final_cumulative_df.write.mode('overwrite').parquet(model3_path)
                # Jika hanya ada 1 batch, model_3 akan sama dengan model_1
                # Jika hanya ada 2 batch, model_3 akan sama dengan model_2

        except Exception as e:
            print(f"Error processing file {current_batch_file}: {e}")
            # Lanjutkan ke file berikutnya jika ada error
            continue
    
    # Jika ada kurang dari 3 batch, pastikan model terakhir adalah model_3
    if num_total_batches == 1 and len(processed_dfs) > 0:
        model2_path = os.path.join(OUTPUT_DIR, 'model_2')
        processed_dfs[0].write.mode('overwrite').parquet(model2_path)
        model3_path = os.path.join(OUTPUT_DIR, 'model_3')
        processed_dfs[0].write.mode('overwrite').parquet(model3_path)
        print("Copied Model 1 data to Model 2 and Model 3 as only one batch was processed.")
    elif num_total_batches == 2 and len(processed_dfs) > 1:
        cumulative_df_model2 = processed_dfs[0].unionByName(processed_dfs[1])
        model3_path = os.path.join(OUTPUT_DIR, 'model_3')
        cumulative_df_model2.write.mode('overwrite').parquet(model3_path)
        print("Copied Model 2 data to Model 3 as only two batches were processed.")


    print("Spark processing finished.")
    spark.stop()

if __name__ == '__main__':
    # Dalam skenario nyata, ini bisa dijadwalkan atau dipicu.
    # Untuk contoh ini, kita jalankan secara manual setelah consumer menghasilkan beberapa batch.
    # Tunggu beberapa saat agar consumer sempat membuat batch files
    print("Spark Processor starting in 10 seconds to allow consumer to create batches...")
   
    main()