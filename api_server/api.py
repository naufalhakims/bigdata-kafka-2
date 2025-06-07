from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.feature import StandardScalerModel, VectorAssembler
from pyspark.sql.types import StructType, StructField, DoubleType, StringType 
import os
import traceback
import pandas as pd
from sklearn.neighbors import NearestNeighbors
import numpy as np

app = Flask(__name__)

# --- CONFIGURATION ---
MODELS_BASE_DIR = os.getenv("MODELS_DIR", "/app/models")
NUM_MODELS_TOTAL = 5

MODEL_TYPES = {
    1: "clustering", 2: "clustering", 3: "recommendation",
    4: "regression", 5: "classification"
}

# Define the exact raw feature columns the API expects for each model type's input.
# These lists must match the 'inputCols' of the VectorAssembler used during
# training for pipeline models, or for the explicit assembler in API for others.
API_INPUT_FEATURES = {
    "clustering": [ # For M1, M2. Used to assemble RAW_FEATURES_COL, then scaled.
        "Protein-G", "Total lipid (fat)-G", "Carbohydrate, by difference-G", "Energy-KCAL",
        "Sugars, total including NLEA-G", "Fiber, total dietary-G", "Calcium, Ca-MG", "Iron, Fe-MG",
        "Sodium, Na-MG", "Vitamin D (D2 + D3)-UG", "Cholesterol-MG", "Fatty acids, total saturated-G",
        "Potassium, K-MG", "Vitamin C, total ascorbic acid-MG", "Vitamin B-6-MG",
        "Vitamin B-12-UG", "Zinc, Zn-MG"
    ],
    "recommendation": [ # For M3. Used to assemble RAW_FEATURES_COL, then scaled for KNN.
        "Protein-G", "Total lipid (fat)-G", "Carbohydrate, by difference-G", "Energy-KCAL",
        "Sugars, total including NLEA-G", "Fiber, total dietary-G", "Calcium, Ca-MG", "Iron, Fe-MG",
        "Sodium, Na-MG", "Vitamin D (D2 + D3)-UG", "Cholesterol-MG", "Fatty acids, total saturated-G",
        "Potassium, K-MG", "Vitamin C, total ascorbic acid-MG", "Vitamin B-6-MG",
        "Vitamin B-12-UG", "Zinc, Zn-MG"
    ],
    "regression": [ # For M4. These are the direct inputs to the saved regression pipeline.
        "Protein-G", "Total lipid (fat)-G", "Carbohydrate, by difference-G"
    ],
    "classification": [ # For M5. These are the direct inputs to the saved classification pipeline.
        "Total lipid (fat)-G", "Carbohydrate, by difference-G", "Sugars, total including NLEA-G", "Sodium, Na-MG"
    ]
}
FOOD_DESCRIPTION_COL = "description" # For Model 3
OUTPUT_FEATURES_COL = "scaled_features" # Scaled vector col name
RAW_FEATURES_COL = "features" # Raw vector col name (used by scalers as input, or by pipelines internally)

spark = None
try:
    spark = SparkSession.builder.appName("UnifiedModelAPI") \
        .master("local[*]") \
        .config("spark.ui.port", os.getenv("SPARK_UI_PORT", "4052")) \
        .config("spark.sql.codegen.wholeStage", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("SparkSession initialized successfully for API.")
except Exception as e:
    print(f"FATAL: Error initializing SparkSession for API: {e}"); traceback.print_exc(); spark = None

# --- Model Storage ---
loaded_scalers = {} # Key: model_id (1,2,3)
loaded_kmeans_models = {} # Key: model_id (1,2)
loaded_regression_pipelines = {} # Key: model_id (4)
loaded_classification_pipelines = {} # Key: model_id (5)
reco_data_for_knn = None # Pandas DF for Model 3
reco_knn_model = None    # Sklearn KNN model for Model 3

def load_all_models_data():
    global loaded_scalers, loaded_kmeans_models, reco_data_for_knn, reco_knn_model
    global loaded_regression_pipelines, loaded_classification_pipelines
    if not spark: print("API Critical: Spark not available for loading models."); return False
    
    print(f"Attempting to load all models/data from: {MODELS_BASE_DIR}")
    models_fully_operational = 0

    for model_id in range(1, NUM_MODELS_TOTAL + 1):
        model_type = MODEL_TYPES.get(model_id)
        print(f"\n--- Loading Model ID {model_id} (Type: {model_type}) ---")
        
        is_current_model_op = False # Flag for this specific model_id
        try:
            if model_type in ["clustering", "recommendation"]:
                scaler_path = os.path.join(MODELS_BASE_DIR, f"scaler_model_{model_id}")
                if os.path.exists(scaler_path) and os.path.isdir(scaler_path):
                    loaded_scalers[model_id] = StandardScalerModel.load(scaler_path)
                    print(f"  Scaler for model {model_id} loaded.")
                else:
                    print(f"  Scaler for model {model_id} not found at {scaler_path}.")
                    raise FileNotFoundError(f"Scaler missing for model {model_id}")

            if model_type == "clustering":
                kmeans_path = os.path.join(MODELS_BASE_DIR, f"kmeans_model_{model_id}")
                if os.path.exists(kmeans_path) and os.path.isdir(kmeans_path) and model_id in loaded_scalers:
                    loaded_kmeans_models[model_id] = KMeansModel.load(kmeans_path)
                    print(f"  KMeans model {model_id} loaded.")
                    is_current_model_op = True
                else: print(f"  KMeans model {model_id} or its scaler not found/loaded.")
            
            elif model_type == "recommendation": # Model 3
                reco_data_path = os.path.join(MODELS_BASE_DIR, f"recommendation_data_model_{model_id}.parquet")
                if os.path.exists(reco_data_path) and model_id in loaded_scalers: # Parquet is a dir
                    spark_reco_df = spark.read.parquet(reco_data_path)
                    temp_reco_list = []
                    # OUTPUT_FEATURES_COL should be the name of the scaled feature vector in the parquet
                    for row_data in spark_reco_df.select(FOOD_DESCRIPTION_COL, OUTPUT_FEATURES_COL).collect():
                        desc = row_data[FOOD_DESCRIPTION_COL]
                        feat_vector = row_data[OUTPUT_FEATURES_COL]
                        temp_reco_list.append({'description': desc, 'features': feat_vector.toArray()})
                    
                    if not temp_reco_list: raise ValueError("No data collected from recommendation Parquet.")
                    reco_data_for_knn = pd.DataFrame(temp_reco_list)
                    X_reco = np.vstack(reco_data_for_knn['features'].values)
                    if X_reco.size == 0: raise ValueError("Feature matrix for KNN is empty.")
                    reco_knn_model = NearestNeighbors(n_neighbors=5, algorithm='auto', metric='cosine').fit(X_reco)
                    print(f"  Recommendation data ({len(reco_data_for_knn)} items) and KNN model for model {model_id} loaded & fitted.")
                    is_current_model_op = True
                else: print(f"  Recommendation data parquet or scaler for model {model_id} not found/loaded.")

            elif model_type == "regression": # Model 4
                reg_pipeline_path = os.path.join(MODELS_BASE_DIR, f"regression_model_{model_id}")
                if os.path.exists(reg_pipeline_path) and os.path.isdir(reg_pipeline_path):
                    loaded_regression_pipelines[model_id] = PipelineModel.load(reg_pipeline_path)
                    print(f"  Regression pipeline model {model_id} loaded.")
                    is_current_model_op = True
                else: print(f"  Regression pipeline model {model_id} not found at {reg_pipeline_path}.")

            elif model_type == "classification": # Model 5
                class_pipeline_path = os.path.join(MODELS_BASE_DIR, f"classification_model_{model_id}")
                if os.path.exists(class_pipeline_path) and os.path.isdir(class_pipeline_path):
                    loaded_classification_pipelines[model_id] = PipelineModel.load(class_pipeline_path)
                    print(f"  Classification pipeline model {model_id} loaded.")
                    is_current_model_op = True
                else: print(f"  Classification pipeline model {model_id} not found at {class_pipeline_path}.")
            
            if is_current_model_op:
                models_fully_operational += 1

        except Exception as load_err:
            print(f"  Failed to load model {model_id} components: {load_err}")
            traceback.print_exc()
            # Clear any partially loaded components for this model_id to ensure clean state
            if model_id in loaded_scalers: del loaded_scalers[model_id]
            if model_id in loaded_kmeans_models: del loaded_kmeans_models[model_id]
            if model_type == "recommendation": reco_data_for_knn = None; reco_knn_model = None
            if model_id in loaded_regression_pipelines: del loaded_regression_pipelines[model_id]
            if model_id in loaded_classification_pipelines: del loaded_classification_pipelines[model_id]
            
    if models_fully_operational == 0 and NUM_MODELS_TOTAL > 0:
        print("CRITICAL: No models became operational."); return False
    if models_fully_operational < NUM_MODELS_TOTAL:
        print(f"Warning: Only {models_fully_operational}/{NUM_MODELS_TOTAL} models are operational.")
    return True

def create_api_input_df(data_json, expected_feature_cols_list):
    if not data_json: raise ValueError("No input data provided from request.")
    input_values = []
    processed_echo_input = {}
    for feature_name in expected_feature_cols_list:
        val = float(data_json.get(feature_name, 0.0))
        input_values.append(val)
        processed_echo_input[feature_name] = val
    api_input_schema_fields = [StructField(feature, DoubleType(), True) for feature in expected_feature_cols_list]
    api_input_schema = StructType(api_input_schema_fields)
    input_spark_df = spark.createDataFrame([input_values], schema=api_input_schema)
    return input_spark_df, processed_echo_input

@app.route('/predict/<int:model_id>', methods=['POST'])
def predict(model_id):
    if not spark: return jsonify({"error": "Spark session not available"}), 500
    if not (1 <= model_id <= NUM_MODELS_TOTAL):
        return jsonify({"error": f"Invalid model_id. Must be 1-{NUM_MODELS_TOTAL}."}), 400

    model_type = MODEL_TYPES.get(model_id)
    if not model_type: return jsonify({"error": "Model type not defined for this ID."}), 500

    expected_api_features = API_INPUT_FEATURES.get(model_type)
    if not expected_api_features:
        return jsonify({"error": f"API input features config not defined for model type '{model_type}'."}), 500

    try:
        data_json = request.get_json()
        current_input_df, processed_echo_input = create_api_input_df(data_json, expected_api_features)
        response_payload = {"model_id": model_id, "model_type": model_type, "input_processed": processed_echo_input}

        if model_type == "clustering":
            if model_id not in loaded_kmeans_models or model_id not in loaded_scalers:
                return jsonify({"error": f"Clustering model/scaler {model_id} not loaded."}), 404
            assembler = VectorAssembler(inputCols=expected_api_features, outputCol=RAW_FEATURES_COL, handleInvalid="keep")
            assembled_df = assembler.transform(current_input_df)
            scaler = loaded_scalers[model_id]
            kmeans_model = loaded_kmeans_models[model_id]
            scaled_df = scaler.transform(assembled_df) # Scaler uses RAW_FEATURES_COL -> OUTPUT_FEATURES_COL
            prediction = kmeans_model.transform(scaled_df) # KMeans uses OUTPUT_FEATURES_COL
            response_payload["cluster"] = prediction.select("prediction").first()[0]

        elif model_type == "recommendation":
            if not reco_knn_model or reco_data_for_knn is None or model_id not in loaded_scalers:
                return jsonify({"error": f"Recommendation model/data/scaler {model_id} not loaded."}), 404
            assembler = VectorAssembler(inputCols=expected_api_features, outputCol=RAW_FEATURES_COL, handleInvalid="keep")
            assembled_df = assembler.transform(current_input_df)
            scaler = loaded_scalers[model_id]
            scaled_df = scaler.transform(assembled_df) # Scaler uses RAW_FEATURES_COL -> OUTPUT_FEATURES_COL
            input_vector_np = scaled_df.select(OUTPUT_FEATURES_COL).first()[0].toArray().reshape(1, -1)
            distances, indices = reco_knn_model.kneighbors(input_vector_np)
            recommendations = [{"description": reco_data_for_knn.iloc[idx]['description'], "distance": float(dist)}
                               for idx, dist in zip(indices[0], distances[0])]
            response_payload["recommendations"] = recommendations

        elif model_type == "regression":
            if model_id not in loaded_regression_pipelines:
                return jsonify({"error": f"Regression model {model_id} not loaded."}), 404
            pipeline_model = loaded_regression_pipelines[model_id]
            # `current_input_df` has the raw columns expected by the pipeline's internal assembler
            prediction = pipeline_model.transform(current_input_df) 
            response_payload["predicted_energy_kcal"] = round(prediction.select("prediction").first()[0], 2)

        elif model_type == "classification":
            if model_id not in loaded_classification_pipelines:
                return jsonify({"error": f"Classification model {model_id} not loaded."}), 404
            pipeline_model = loaded_classification_pipelines[model_id]
            prediction_result = pipeline_model.transform(current_input_df).first()
            predicted_label = int(prediction_result["prediction"])
            probability = prediction_result["probability"].toArray().tolist()
            response_payload["is_high_protein"] = predicted_label
            response_payload["probability_is_high_protein"] = round(probability[1], 4) if len(probability)==2 else "N/A"
            # response_payload["probability_vector"] = [round(p, 4) for p in probability] # Optional to include full vector

        return jsonify(response_payload)

    except ValueError as ve: return jsonify({"error": str(ve)}), 400
    except Exception as e:
        app.logger.error(f"Prediction error for model {model_id} ({model_type}): {e}"); traceback.print_exc()
        return jsonify({"error": "Prediction failed", "details": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    if not spark: return jsonify({"status": "unhealthy", "reason": "Spark not initialized"}), 503 # 503 for service unavailable
    
    op_models = 0
    details = {}
    for mid in range(1, NUM_MODELS_TOTAL + 1):
        mtype = MODEL_TYPES.get(mid)
        is_op = False
        if mtype == "clustering" and mid in loaded_kmeans_models and mid in loaded_scalers: is_op = True
        elif mtype == "recommendation" and reco_knn_model and reco_data_for_knn is not None and mid in loaded_scalers: is_op = True
        elif mtype == "regression" and mid in loaded_regression_pipelines: is_op = True
        elif mtype == "classification" and mid in loaded_classification_pipelines: is_op = True
        details[f"model_{mid}_{mtype}"] = "operational" if is_op else "not_operational"
        if is_op: op_models +=1

    status = "healthy"
    if op_models == 0 and NUM_MODELS_TOTAL > 0: status = "unhealthy"
    elif op_models < NUM_MODELS_TOTAL: status = "degraded"
        
    http_code = 200
    if status == "unhealthy": http_code = 503
    elif status == "degraded": http_code = 200 # Could also be 503 if degradation is critical

    return jsonify({
        "overall_status": status,
        "operational_models": op_models,
        "total_expected_models": NUM_MODELS_TOTAL,
        "details": details
    }), http_code

if __name__ == '__main__':
    print("Initializing API server...")
    load_all_models_data()
    if spark: app.run(host='0.0.0.0', port=5000, debug=False) # debug=False for stability
    else: print("Flask app NOT starting: Spark Session failed to initialize.")