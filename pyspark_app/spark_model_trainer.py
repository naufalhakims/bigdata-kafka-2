from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import LinearRegression, GBTRegressor # Added GBTRegressor
from pyspark.ml.classification import LogisticRegression, GBTClassifier # Added GBTClassifier
from pyspark.ml import Pipeline # For classification/regression pipelines
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import os

# --- CONFIGURATION ---
NUM_MODELS_TO_TRAIN = 5
MODELS_DIR = "/app/data/models"
BATCHES_DIR = "/app/data/batches"

# Columns needed from CSV (master list, producer should send these)
BASE_SCHEMA_COLUMNS = [
    "Protein-G", "Total lipid (fat)-G", "Carbohydrate, by difference-G", "Energy-KCAL",
    "Sugars, total including NLEA-G", "Fiber, total dietary-G", "Calcium, Ca-MG", "Iron, Fe-MG",
    "Sodium, Na-MG", "Vitamin D (D2 + D3)-UG", "Cholesterol-MG", "Fatty acids, total saturated-G",
    "Potassium, K-MG", "Vitamin C, total ascorbic acid-MG", "Vitamin B-6-MG",
    "Vitamin B-12-UG", "Zinc, Zn-MG"
]
FOOD_DESCRIPTION_COL = "description"
OUTPUT_FEATURES_COL = "scaled_features" # For scaled vector
RAW_FEATURES_COL = "features" # For raw vector before scaling

# Model-specific configurations
# Model 1 & 2: Clustering
M12_CLUSTERING_FEATURES = BASE_SCHEMA_COLUMNS # Use all base numeric for clustering
M12_NUM_CLUSTERS = 5

# Model 3: Recommendation (Content-Based KNN)
M3_RECO_FEATURES = BASE_SCHEMA_COLUMNS # Base features for similarity

# Model 4: Energy Prediction (Regression)
M4_REGRESSION_TARGET = "Energy-KCAL"
M4_REGRESSION_FEATURES = ["Protein-G", "Total lipid (fat)-G", "Carbohydrate, by difference-G"]

# Model 5: High Nutrient Classifier
M5_CLASSIFICATION_TARGET_RAW = "Protein-G" # Feature to derive label from
M5_CLASSIFICATION_LABEL = "is_high_protein" # New binary label column
M5_HIGH_PROTEIN_THRESHOLD = 20.0 # e.g., > 20g protein is "high"
M5_CLASSIFICATION_FEATURES = ["Total lipid (fat)-G", "Carbohydrate, by difference-G", "Sugars, total including NLEA-G", "Sodium, Na-MG"]

def prepare_dataframe(df, required_cols, numeric_cols_to_cast, string_cols_to_check):
    """Ensures columns exist, casts numeric, fills NA."""
    for sconf in string_cols_to_check:
        if sconf not in df.columns:
            print(f"Warning: String column '{sconf}' not in DataFrame. Adding as 'Unknown'.")
            df = df.withColumn(sconf, lit("Unknown"))
        else:
            df = df.na.fill("Unknown", subset=[sconf])
            df = df.withColumn(sconf, col(sconf).cast(StringType()))


    for ncol in numeric_cols_to_cast:
        if ncol not in df.columns:
            print(f"Warning: Numeric column '{ncol}' not in DataFrame. Adding as 0.0.")
            df = df.withColumn(ncol, lit(0.0))
        else:
            df = df.withColumn(ncol, col(ncol).cast(DoubleType()))
            df = df.na.fill(0.0, subset=[ncol])
    return df

# --- MODEL TRAINING/PREPARATION FUNCTIONS ---

def train_clustering_model(spark, data_df, model_id_suffix):
    print(f"Training Clustering Model {model_id_suffix}...")
    df = prepare_dataframe(data_df, M12_CLUSTERING_FEATURES, M12_CLUSTERING_FEATURES, [])
    
    if df.count() < M12_NUM_CLUSTERS:
        print(f"Not enough data for KMeans Model {model_id_suffix}. Skipping.")
        return

    assembler = VectorAssembler(inputCols=M12_CLUSTERING_FEATURES, outputCol=RAW_FEATURES_COL, handleInvalid="skip")
    scaler = StandardScaler(inputCol=RAW_FEATURES_COL, outputCol=OUTPUT_FEATURES_COL, withStd=True, withMean=True)
    kmeans = KMeans(featuresCol=OUTPUT_FEATURES_COL, k=M12_NUM_CLUSTERS, seed=1)
    
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    try:
        pipeline_model = pipeline.fit(df)
        # Save scaler (stage 1) and kmeans (stage 2) models from the pipeline
        pipeline_model.stages[1].write().overwrite().save(os.path.join(MODELS_DIR, f"scaler_model_{model_id_suffix}"))
        pipeline_model.stages[2].write().overwrite().save(os.path.join(MODELS_DIR, f"kmeans_model_{model_id_suffix}"))
        print(f"Clustering Model {model_id_suffix} and Scaler saved.")
    except Exception as e:
        print(f"Error training/saving clustering model {model_id_suffix}: {e}")
        import traceback; traceback.print_exc()

def prepare_recommendation_data(spark, data_df, model_id_suffix):
    print(f"Preparing Recommendation Data {model_id_suffix}...")
    df = prepare_dataframe(data_df, M3_RECO_FEATURES + [FOOD_DESCRIPTION_COL], M3_RECO_FEATURES, [FOOD_DESCRIPTION_COL])

    if df.count() == 0:
        print(f"No data for Recommendation model {model_id_suffix}. Skipping.")
        return

    assembler = VectorAssembler(inputCols=M3_RECO_FEATURES, outputCol=RAW_FEATURES_COL, handleInvalid="skip")
    scaler = StandardScaler(inputCol=RAW_FEATURES_COL, outputCol=OUTPUT_FEATURES_COL, withStd=True, withMean=True)
    
    pipeline = Pipeline(stages=[assembler, scaler]) # Only assemble and scale
    try:
        transform_pipeline_model = pipeline.fit(df)
        scaled_df_reco = transform_pipeline_model.transform(df).select(FOOD_DESCRIPTION_COL, OUTPUT_FEATURES_COL)

        transform_pipeline_model.stages[1].write().overwrite().save(os.path.join(MODELS_DIR, f"scaler_model_{model_id_suffix}"))
        
        reco_data_path = os.path.join(MODELS_DIR, f"recommendation_data_model_{model_id_suffix}.parquet")
        scaled_df_reco.write.mode("overwrite").parquet(reco_data_path)
        print(f"Recommendation data and Scaler for {model_id_suffix} saved.")
    except Exception as e:
        print(f"Error preparing/saving recommendation data {model_id_suffix}: {e}")
        import traceback; traceback.print_exc()

def train_regression_model(spark, data_df, model_id_suffix):
    print(f"Training Regression Model {model_id_suffix} for target '{M4_REGRESSION_TARGET}'...")
    required_cols = M4_REGRESSION_FEATURES + [M4_REGRESSION_TARGET]
    df = prepare_dataframe(data_df, required_cols, required_cols, [])

    if df.count() < 10 : # Arbitrary minimum
        print(f"Not enough data for Regression Model {model_id_suffix}. Skipping.")
        return

    assembler = VectorAssembler(inputCols=M4_REGRESSION_FEATURES, outputCol=RAW_FEATURES_COL, handleInvalid="skip")
    # No scaling for tree-based models, but good for LinearRegression. Let's use GBT which is robust.
    # scaler = StandardScaler(inputCol=RAW_FEATURES_COL, outputCol=OUTPUT_FEATURES_COL, withStd=True, withMean=True)
    
    # Using GBTRegressor
    gbt_regressor = GBTRegressor(featuresCol=RAW_FEATURES_COL, labelCol=M4_REGRESSION_TARGET, maxIter=10) # Simple config
    
    pipeline = Pipeline(stages=[assembler, gbt_regressor]) # If using LinearRegression, add scaler before it
    try:
        pipeline_model = pipeline.fit(df)
        # Save the whole pipeline model, it contains assembler and GBT
        pipeline_model.write().overwrite().save(os.path.join(MODELS_DIR, f"regression_model_{model_id_suffix}"))
        # If you needed a separate scaler, save it like:
        # pipeline_model.stages[1].write().overwrite().save(os.path.join(MODELS_DIR, f"scaler_model_{model_id_suffix}")) # If scaler was stage 1
        print(f"Regression Model {model_id_suffix} (Pipeline) saved.")
    except Exception as e:
        print(f"Error training/saving regression model {model_id_suffix}: {e}")
        import traceback; traceback.print_exc()

def train_classification_model(spark, data_df, model_id_suffix):
    print(f"Training Classification Model {model_id_suffix} for label '{M5_CLASSIFICATION_LABEL}'...")
    # Ensure target raw feature and other features are present and numeric
    required_numeric_cols = M5_CLASSIFICATION_FEATURES + [M5_CLASSIFICATION_TARGET_RAW]
    df = prepare_dataframe(data_df, required_numeric_cols, required_numeric_cols, [])

    # Create the binary label
    df = df.withColumn(M5_CLASSIFICATION_LABEL, 
                       when(col(M5_CLASSIFICATION_TARGET_RAW) > M5_HIGH_PROTEIN_THRESHOLD, 1.0)
                       .otherwise(0.0))
    
    # Check class balance (optional but good practice)
    df.groupBy(M5_CLASSIFICATION_LABEL).count().show()
    
    if df.count() < 20: # Arbitrary minimum
        print(f"Not enough data for Classification Model {model_id_suffix}. Skipping.")
        return
    
    assembler = VectorAssembler(inputCols=M5_CLASSIFICATION_FEATURES, outputCol=RAW_FEATURES_COL, handleInvalid="skip")
    # Using GBTClassifier
    gbt_classifier = GBTClassifier(featuresCol=RAW_FEATURES_COL, labelCol=M5_CLASSIFICATION_LABEL, maxIter=10)

    pipeline = Pipeline(stages=[assembler, gbt_classifier])
    try:
        pipeline_model = pipeline.fit(df)
        pipeline_model.write().overwrite().save(os.path.join(MODELS_DIR, f"classification_model_{model_id_suffix}"))
        print(f"Classification Model {model_id_suffix} (Pipeline) saved.")
    except Exception as e:
        print(f"Error training/saving classification model {model_id_suffix}: {e}")
        import traceback; traceback.print_exc()


# --- MAIN SCRIPT ---
if __name__ == "__main__":
    spark = SparkSession.builder.appName("UniversalModelTrainer").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    os.makedirs(MODELS_DIR, exist_ok=True)

    # Define schema for reading CSVs
    schema_fields = [StructField(c, DoubleType(), True) for c in BASE_SCHEMA_COLUMNS]
    if FOOD_DESCRIPTION_COL:
        schema_fields.append(StructField(FOOD_DESCRIPTION_COL, StringType(), True))
    input_csv_schema = StructType(schema_fields)
    print(f"Expected schema for reading batches: {input_csv_schema}")

    try:
        combined_df = spark.read.option("header", "true").schema(input_csv_schema).csv(BATCHES_DIR)
    except Exception as e:
        print(f"Error reading CSVs from {BATCHES_DIR} with defined schema: {e}. Trying inferSchema.")
        try:
            combined_df = spark.read.option("header", "true").option("inferSchema", "true").csv(BATCHES_DIR)
        except Exception as e2:
            print(f"Fallback reading also failed: {e2}. Exiting.")
            spark.stop(); exit()
            
    total_records = combined_df.count()
    print(f"Total records read from all batches: {total_records}")
    combined_df.printSchema()

    if total_records == 0:
        print("No data in batch files. Exiting."); spark.stop(); exit()
    
    combined_df.persist() # Cache for multiple uses

    # Determine records for each cumulative model
    # Model 1: 1/5, Model 2: 2/5, ..., Model 5: 5/5 (all)
    base_increment = total_records // NUM_MODELS_TO_TRAIN if NUM_MODELS_TO_TRAIN > 0 else total_records
    if base_increment == 0 and total_records > 0: base_increment = total_records # Ensure some records if total is small

    for i in range(NUM_MODELS_TO_TRAIN):
        model_id_suffix = i + 1 # Model IDs are 1-based
        
        # Cumulative data: model `k` uses `k/N * total_records`
        if i < NUM_MODELS_TO_TRAIN - 1: # Not the last model
            num_records_for_this_model = base_increment * (i + 1)
        else: # Last model uses all data
            num_records_for_this_model = total_records
        
        num_records_for_this_model = min(num_records_for_this_model, total_records) # Cap at total
        if num_records_for_this_model == 0 and total_records > 0: # Ensure at least some for first model
            num_records_for_this_model = total_records 

        current_data_slice_df = combined_df.limit(num_records_for_this_model) # .limit() is approximate for unsorted data
        print(f"\n--- Processing Model ID {model_id_suffix} with {current_data_slice_df.count()} records (target: {num_records_for_this_model}) ---")

        if model_id_suffix == 1: # Clustering Model 1
            train_clustering_model(spark, current_data_slice_df, model_id_suffix)
        elif model_id_suffix == 2: # Clustering Model 2
            train_clustering_model(spark, current_data_slice_df, model_id_suffix)
        elif model_id_suffix == 3: # Recommendation Data Prep
            prepare_recommendation_data(spark, current_data_slice_df, model_id_suffix)
        elif model_id_suffix == 4: # Regression Model
            train_regression_model(spark, current_data_slice_df, model_id_suffix)
        elif model_id_suffix == 5: # Classification Model
            train_classification_model(spark, current_data_slice_df, model_id_suffix)
        else:
            print(f"Unknown model index: {i}")

    combined_df.unpersist()
    spark.stop()
    print("Spark training/preparation process finished for all models.")