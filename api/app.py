from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

app = Flask(__name__)

PROCESSED_DATA_DIR = '/app/data/processed_data/' # Path Parquet di container
spark_sessions = {} # Cache untuk SparkSession jika diperlukan

def get_spark_session():
    # Menggunakan satu SparkSession untuk seluruh aplikasi bisa lebih efisien
    # Tapi untuk API, seringkali lebih baik jika tidak ada state berat seperti SparkContext penuh
    # Untuk contoh ini, kita buat session per request atau load data tanpa Spark aktif di API
    # Pilihan: load parquet dengan pandas jika data tidak terlalu besar untuk API
    if "default" not in spark_sessions:
        spark_sessions["default"] = SparkSession.builder \
            .appName("FoodAllergenAPI") \
            .master("local[*]") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
    return spark_sessions["default"]

def load_data_for_model(model_version):
    model_path = os.path.join(PROCESSED_DATA_DIR, f'model_{model_version}')
    if not os.path.exists(model_path):
        return None
    try:
        spark = get_spark_session()
        df = spark.read.parquet(model_path)
        # Kolom yang relevan untuk pencarian: fdc_id, description, ingredients_processed
        # Pastikan 'ingredients_processed' ada dari Spark Processor
        return df.select("fdc_id", "description", "ingredients_processed")
    except Exception as e:
        print(f"Error loading data for model {model_version}: {e}")
        return None

@app.route('/find_allergen/model<int:model_version>', methods=['GET'])
def find_allergen_by_model(model_version):
    allergen = request.args.get('allergy')
    if not allergen:
        return jsonify({"error": "Parameter 'allergy' is required"}), 400

    if model_version not in [1, 2, 3]:
        return jsonify({"error": "Invalid model version. Use 1, 2, or 3."}), 400

    df = load_data_for_model(model_version)

    if df is None:
        return jsonify({"error": f"Model {model_version} not found or failed to load."}), 404

    allergen_lower = allergen.lower()
    
    # Lakukan filtering. Pastikan 'ingredients_processed' ada dan non-null
    # `col("ingredients_processed").contains(allergen_lower)`
    # `instr` bisa lebih aman jika `ingredients_processed` bisa null setelah seleksi (meskipun kita sudah handle di processor)
    results_df = df.filter(col("ingredients_processed").isNotNull() & col("ingredients_processed").contains(allergen_lower))
    
    # Ambil beberapa hasil (misal, 20 teratas) untuk menghindari response besar
    results = results_df.select("fdc_id", "description").limit(20).collect()
    
    # Konversi hasil Row ke dictionary
    output = [{"fdc_id": row.fdc_id, "description": row.description} for row in results]

    return jsonify({
        "model_version": model_version,
        "allergen_searched": allergen,
        "matching_foods": output,
        "count": len(output)
    })

# Contoh endpoint tambahan (sesuai permintaan "minimal sebanyak jumlah anggota")
@app.route('/food_details/model<int:model_version>/<string:fdc_id>', methods=['GET'])
def get_food_details(model_version, fdc_id):
    if model_version not in [1, 2, 3]:
        return jsonify({"error": "Invalid model version. Use 1, 2, or 3."}), 400

    df = load_data_for_model(model_version)
    if df is None:
        return jsonify({"error": f"Model {model_version} not found or failed to load."}), 404

    food_detail_df = df.filter(col("fdc_id") == fdc_id)
    food_detail = food_detail_df.select("fdc_id", "description", "ingredients_processed").first()

    if food_detail:
        return jsonify({
            "model_version": model_version,
            "fdc_id": food_detail.fdc_id,
            "description": food_detail.description,
            "ingredients": food_detail.ingredients_processed # Tampilkan ingredients yang sudah diproses
        })
    else:
        return jsonify({"error": f"Food with fdc_id {fdc_id} not found in model {model_version}."}), 404

@app.route('/stats/model<int:model_version>', methods=['GET'])
def get_model_stats(model_version):
    if model_version not in [1, 2, 3]:
        return jsonify({"error": "Invalid model version. Use 1, 2, or 3."}), 400

    df = load_data_for_model(model_version)
    if df is None:
        return jsonify({"error": f"Model {model_version} not found or failed to load."}), 404
    
    count = df.count()
    return jsonify({
        "model_version": model_version,
        "total_records": count
    })


if __name__ == '__main__':
    # Tunggu Spark Processor selesai (dalam skenario nyata, API akan dijalankan setelah data siap)
    # Untuk Docker, API bisa mulai kapan saja, tapi endpoint mungkin error jika data belum ada.
    # Kita bisa menambahkan pemeriksaan kesiapan data di sini.
    print("API Service starting. Ensure Spark processor has run and data is available.")
    app.run(host='0.0.0.0', port=5000, debug=True) # debug=False untuk produksi