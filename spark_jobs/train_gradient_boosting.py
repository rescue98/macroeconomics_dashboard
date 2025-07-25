#!/usr/bin/env python3
"""
Spark ML job for training Gradient Boosting Regressor
Combines Chilean export data and World Bank GDP data for economic prediction
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import logging
import json
from datetime import datetime
import pickle
import numpy as np
from io import BytesIO
from minio import Minio

def create_spark_session():
    """Create Spark session with ML and MinIO configuration"""
    spark = SparkSession.builder \
        .appName("GradientBoostingGDPPrediction") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_processed_data(spark):
    """Load and combine processed export and GDP data"""
    print("Loading processed data from MinIO...")
    
    try:
        # Load World Bank GDP data
        wb_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("s3a://etl-data/processed/worldbank_data/*.csv")
        
        print(f"Loaded {wb_df.count()} World Bank GDP records")
        
        # Load Chilean export data
        export_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("s3a://etl-data/processed/local_data/processed_export_data*.csv")
        
        print(f"Loaded {export_df.count()} Chilean export records")
        
        return wb_df, export_df
        
    except Exception as e:
        print(f"Error loading processed data: {str(e)}")
        raise

def create_features_dataset(wb_df, export_df):
    """Create comprehensive feature dataset for ML training"""
    print("Creating comprehensive feature dataset...")
    
    # Filter and prepare World Bank data
    wb_clean = wb_df.filter(
        (col("gdp_value").isNotNull()) &
        (col("gdp_value") > 0) &
        (col("year").isNotNull()) &
        (col("year") >= 2019) &
        (col("year") <= 2024)
    ).select(
        "country_code", "country_name", "year", "gdp_value", 
        "ranking", "gdp_growth_rate", "gdp_billion_usd", "log_gdp"
    )
    
    # Aggregate Chilean export data by year (focusing on total export performance)
    if export_df.count() > 0:
        export_agg = export_df.filter(
            (col("YEAR").isNotNull()) &
            (col("FOB_CLEAN").isNotNull()) &
            (col("FOB_CLEAN") > 0)
        ).groupBy("YEAR").agg(
            sum("FOB_CLEAN").alias("total_exports_usd"),
            avg("FOB_CLEAN").alias("avg_export_value"),
            count("*").alias("export_transactions"),
            countDistinct("PAIS_DESTINO_CLEAN").alias("export_destinations"),
            countDistinct("PRODUCTO_CLEAN").alias("export_products"),
            countDistinct("REGION_ORIGEN_CLEAN").alias("export_regions")
        ).withColumnRenamed("YEAR", "year")
        
        print(f"Created export aggregations for {export_agg.count()} years")
    else:
        # Create empty export aggregation if no export data
        export_schema = StructType([
            StructField("year", IntegerType(), True),
            StructField("total_exports_usd", DoubleType(), True),
            StructField("avg_export_value", DoubleType(), True),
            StructField("export_transactions", LongType(), True),
            StructField("export_destinations", LongType(), True),
            StructField("export_products", LongType(), True),
            StructField("export_regions", LongType(), True)
        ])
        export_agg = spark.createDataFrame([], export_schema)
        print("No export data available - using empty export features")
    
    # Join World Bank data with export aggregations
    # Focus on countries with significant economies for better prediction
    major_economies = wb_clean.filter(col("gdp_billion_usd") >= 10)  # GDP >= $10B
    
    # Left join to keep all major economies, fill missing export data with 0
    combined_df = major_economies.join(export_agg, on="year", how="left")
    
    # Fill missing export values
    combined_df = combined_df.fillna({
        "total_exports_usd": 0,
        "avg_export_value": 0,
        "export_transactions": 0,
        "export_destinations": 0,
        "export_products": 0,
        "export_regions": 0
    })
    
    print(f"Combined dataset: {combined_df.count()} records for training")
    return combined_df

def engineer_features(df):
    """Engineer features for gradient boosting model"""
    print("Engineering features for ML model...")
    
    # Sort by country and year for lag features
    window_country = Window.partitionBy("country_code").orderBy("year")
    
    # 1. Historical features (lags)
    df = df.withColumn("gdp_lag_1", lag("gdp_value", 1).over(window_country))
    df = df.withColumn("gdp_lag_2", lag("gdp_value", 2).over(window_country))
    df = df.withColumn("gdp_growth_lag_1", lag("gdp_growth_rate", 1).over(window_country))
    
    # 2. Moving averages
    window_3yr = Window.partitionBy("country_code").orderBy("year").rowsBetween(-2, 0)
    df = df.withColumn("gdp_ma_3yr", avg("gdp_value").over(window_3yr))
    df = df.withColumn("gdp_growth_ma_3yr", avg("gdp_growth_rate").over(window_3yr))
    
    # 3. Ranking features
    df = df.withColumn("ranking_change", col("ranking") - lag("ranking", 1).over(window_country))
    df = df.withColumn("ranking_normalized", col("ranking") / 200.0)  # Normalize assuming ~200 countries
    
    # 4. Economic development indicators
    df = df.withColumn("gdp_per_capita_proxy", col("gdp_billion_usd") / (col("ranking") + 1))
    df = df.withColumn("economic_momentum", 
                      when(col("gdp_growth_rate") > 5, 3)
                      .when(col("gdp_growth_rate") > 2, 2)
                      .when(col("gdp_growth_rate") > 0, 1)
                      .otherwise(0))
    
    # 5. Export-related features (interaction with GDP)
    df = df.withColumn("export_gdp_ratio", 
                      when(col("gdp_value") > 0, col("total_exports_usd") / col("gdp_value"))
                      .otherwise(0))
    
    df = df.withColumn("export_diversity_score", 
                      col("export_destinations") * col("export_products") * 0.001)
    
    # 6. Time-based features
    df = df.withColumn("years_since_2019", col("year") - 2019)
    df = df.withColumn("year_squared", col("year") * col("year"))
    
    # 7. Regional economic context (simplified)
    df = df.withColumn("is_major_economy", when(col("gdp_billion_usd") >= 1000, 1).otherwise(0))
    df = df.withColumn("is_emerging_market", 
                      when((col("gdp_billion_usd") >= 100) & (col("gdp_billion_usd") < 1000), 1)
                      .otherwise(0))
    
    # 8. Volatility measures
    df = df.withColumn("gdp_volatility", 
                      stddev("gdp_growth_rate").over(window_3yr))
    
    # Fill missing values for lag features
    df = df.fillna({
        "gdp_lag_1": 0,
        "gdp_lag_2": 0,
        "gdp_growth_lag_1": 0,
        "ranking_change": 0,
        "gdp_volatility": 0
    })
    
    print("Feature engineering completed")
    return df

def prepare_ml_dataset(df):
    """Prepare final dataset for ML training"""
    print("Preparing ML dataset...")
    
    # Select features for training
    feature_columns = [
        "year", "years_since_2019", "year_squared",
        "ranking", "ranking_normalized", "ranking_change",
        "gdp_growth_rate", "gdp_growth_lag_1", "gdp_growth_ma_3yr",
        "gdp_lag_1", "gdp_lag_2", "gdp_ma_3yr",
        "gdp_per_capita_proxy", "economic_momentum",
        "total_exports_usd", "export_transactions", "export_destinations",
        "export_gdp_ratio", "export_diversity_score",
        "is_major_economy", "is_emerging_market", "gdp_volatility"
    ]
    
    # Target variable
    target_column = "gdp_billion_usd"
    
    # Filter out rows with missing lag features (first year for each country)
    ml_df = df.filter(col("gdp_lag_1").isNotNull() & (col("gdp_lag_1") > 0))
    
    # Select only the columns we need
    final_columns = feature_columns + [target_column, "country_code", "year"]
    ml_df = ml_df.select(*final_columns)
    
    # Remove any remaining null values
    ml_df = ml_df.dropna()
    
    print(f"Final ML dataset: {ml_df.count()} records with {len(feature_columns)} features")
    return ml_df, feature_columns

def train_gradient_boosting_model(df, feature_columns):
    """Train Gradient Boosting Regressor with hyperparameter tuning"""
    print("Training Gradient Boosting Regressor...")
    
    # Prepare features
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    
    # Scale features
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", 
                           withStd=True, withMean=True)
    
    # Gradient Boosting Regressor
    gbt = GBTRegressor(
        featuresCol="scaled_features",
        labelCol="gdp_billion_usd",
        predictionCol="prediction",
        maxIter=100,
        stepSize=0.1,
        maxDepth=5,
        seed=42
    )
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    # Split data for training and testing
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    
    print(f"Training set: {train_df.count()} records")
    print(f"Test set: {test_df.count()} records")
    
    # Train the model
    print("Training model...")
    model = pipeline.fit(train_df)
    
    # Make predictions on test set
    predictions = model.transform(test_df)
    
    # Evaluate model
    evaluator = RegressionEvaluator(
        labelCol="gdp_billion_usd", 
        predictionCol="prediction",
        metricName="r2"
    )
    
    r2_score = evaluator.evaluate(predictions)
    
    # Calculate additional metrics
    rmse_evaluator = RegressionEvaluator(
        labelCol="gdp_billion_usd", 
        predictionCol="prediction",
        metricName="rmse"
    )
    rmse_score = rmse_evaluator.evaluate(predictions)
    
    mae_evaluator = RegressionEvaluator(
        labelCol="gdp_billion_usd", 
        predictionCol="prediction",
        metricName="mae"
    )
    mae_score = mae_evaluator.evaluate(predictions)
    
    print(f"Model Performance:")
    print(f"  R² Score: {r2_score:.4f}")
    print(f"  RMSE: {rmse_score:.2f}")
    print(f"  MAE: {mae_score:.2f}")
    
    return model, r2_score, rmse_score, mae_score, test_df

def save_model_artifacts(model, feature_columns, r2_score, rmse_score, mae_score, test_df):
    """Save model and related artifacts to MinIO"""
    print("Saving model artifacts to MinIO...")
    
    # Initialize MinIO client
    minio_client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    bucket_name = 'etl-data'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        # Extract trained model components
        trained_pipeline = model
        gbt_model = trained_pipeline.stages[-1]  # GBT is the last stage
        
        # Get feature importance
        feature_importance = gbt_model.featureImportances.toArray()
        importance_dict = [
            {"feature": feature_columns[i], "importance": float(feature_importance[i])}
            for i in range(len(feature_columns))
        ]
        importance_dict = sorted(importance_dict, key=lambda x: x["importance"], reverse=True)
        
        # Save feature importance
        importance_json = json.dumps(importance_dict, indent=2)
        minio_client.put_object(
            bucket_name,
            f"models/feature_importance_{timestamp}.json",
            data=BytesIO(importance_json.encode('utf-8')),
            length=len(importance_json.encode('utf-8')),
            content_type='application/json'
        )
        
        # Save model metadata
        metadata = {
            "model_name": "gradient_boosting_gdp_predictor",
            "model_version": timestamp,
            "model_type": "gradient_boosting_regressor",
            "training_date": datetime.now().isoformat(),
            "feature_names": feature_columns,
            "performance_metrics": {
                "r2_score": float(r2_score),
                "rmse": float(rmse_score),
                "mae": float(mae_score)
            },
            "training_samples": test_df.count(),
            "hyperparameters": {
                "max_iter": 100,
                "step_size": 0.1,
                "max_depth": 5
            }
        }
        
        metadata_json = json.dumps(metadata, indent=2)
        minio_client.put_object(
            bucket_name,
            f"models/model_metadata_{timestamp}.json",
            data=BytesIO(metadata_json.encode('utf-8')),
            length=len(metadata_json.encode('utf-8')),
            content_type='application/json'
        )
        
        # Save sample predictions for validation
        sample_predictions = test_df.limit(20).toPandas()
        sample_csv = sample_predictions.to_csv(index=False)
        minio_client.put_object(
            bucket_name,
            f"models/sample_predictions_{timestamp}.csv",
            data=BytesIO(sample_csv.encode('utf-8')),
            length=len(sample_csv.encode('utf-8')),
            content_type='text/csv'
        )
        
        print(f"Model artifacts saved successfully with timestamp: {timestamp}")
        print(f"Top 5 important features:")
        for i, item in enumerate(importance_dict[:5]):
            print(f"  {i+1}. {item['feature']}: {item['importance']:.4f}")
        
        return timestamp
        
    except Exception as e:
        print(f"Error saving model artifacts: {str(e)}")
        raise

def main():
    """Main execution function"""
    print("=== Starting Gradient Boosting GDP Prediction Training ===")
    
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Load processed data
        wb_df, export_df = load_processed_data(spark)
        
        # Create feature dataset
        combined_df = create_features_dataset(wb_df, export_df)
        
        # Engineer features
        features_df = engineer_features(combined_df)
        
        # Prepare ML dataset
        ml_df, feature_columns = prepare_ml_dataset(features_df)
        
        if ml_df.count() < 50:
            raise ValueError(f"Insufficient data for training: {ml_df.count()} records")
        
        # Train model
        model, r2_score, rmse_score, mae_score, test_df = train_gradient_boosting_model(ml_df, feature_columns)
        
        # Save artifacts
        model_version = save_model_artifacts(model, feature_columns, r2_score, rmse_score, mae_score, test_df)
        
        print(f"\n=== Training Summary ===")
        print(f"Model Version: {model_version}")
        print(f"Final Model Performance:")
        print(f"  • R² Score: {r2_score:.4f}")
        print(f"  • RMSE: {rmse_score:.2f} billion USD")
        print(f"  • MAE: {mae_score:.2f} billion USD")
        print(f"Training completed successfully!")
        
    except Exception as e:
        print(f"Training failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()