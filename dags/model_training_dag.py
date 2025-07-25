from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
import logging
from minio import Minio
from io import BytesIO
import pickle
import joblib
import json
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import warnings
warnings.filterwarnings('ignore')

# Default arguments
default_args = {
    'owner': 'ml-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

# DAG definition
dag = DAG(
    'ml_model_training_pipeline',
    default_args=default_args,
    description='ML pipeline for GDP prediction model training',
    schedule_interval=timedelta(days=30),  # Monthly retraining
    catchup=False,
    tags=['ml', 'training', 'gdp', 'regression', 'gbr']
)

def extract_training_data():
    """Extract and prepare data for ML training"""
    logging.info("Starting data extraction for ML training...")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Extract combined dataset for training
    query = """
    WITH local_data_agg AS (
        SELECT 
            "YEAR" as year,
            AVG(CASE WHEN "US$ FOB" > 0 THEN "US$ FOB" ELSE NULL END) as avg_fob_value,
            AVG(CASE WHEN "PESO BRUTO" > 0 THEN "PESO BRUTO" ELSE NULL END) as avg_weight,
            COUNT(*) as export_records,
            COUNT(DISTINCT "PAIS DE DESTINO") as unique_destinations,
            COUNT(DISTINCT "PRODUCTO") as unique_products,
            COUNT(DISTINCT "REGION DE ORIGEN") as unique_regions
        FROM etl_schema.local_data
        WHERE "YEAR" IS NOT NULL 
        AND "US$ FOB" IS NOT NULL
        GROUP BY "YEAR"
    ),
    worldbank_data AS (
        SELECT 
            country_code,
            country_name,
            year,
            gdp_value,
            ranking,
            gdp_growth_rate,
            region,
            economy_size,
            COALESCE(gdp_growth_rate, 0) as gdp_growth_rate_filled
        FROM etl_schema.world_bank_gdp
        WHERE gdp_value IS NOT NULL
        AND gdp_value > 0
    )
    SELECT 
        wb.country_code,
        wb.country_name,
        wb.year,
        wb.gdp_value,
        wb.ranking,
        wb.gdp_growth_rate_filled as gdp_growth_rate,
        wb.region,
        wb.economy_size,
        COALESCE(ld.avg_fob_value, 0) as local_avg_fob,
        COALESCE(ld.avg_weight, 0) as local_avg_weight,
        COALESCE(ld.export_records, 0) as local_export_records,
        COALESCE(ld.unique_destinations, 0) as local_unique_destinations,
        COALESCE(ld.unique_products, 0) as local_unique_products,
        COALESCE(ld.unique_regions, 0) as local_unique_regions,
        LOG(wb.gdp_value) as log_gdp,
        wb.gdp_value / 1000000000.0 as gdp_billions
    FROM worldbank_data wb
    LEFT JOIN local_data_agg ld ON wb.year = ld.year
    ORDER BY wb.year, wb.ranking
    """
    
    try:
        df = pd.read_sql(query, postgres_hook.get_sqlalchemy_engine())
        
        if df.empty:
            raise ValueError("No training data available")
        
        logging.info(f"Extracted {len(df)} records for training")
        logging.info(f"Countries: {df['country_code'].nunique()}")
        logging.info(f"Years: {sorted(df['year'].unique())}")
        logging.info(f"GDP range: ${df['gdp_value'].min():,.0f} - ${df['gdp_value'].max():,.0f}")
        
        return df
        
    except Exception as e:
        logging.error(f"Error extracting training data: {str(e)}")
        raise

def feature_engineering(df):
    """Apply feature engineering for ML training"""
    logging.info("Starting feature engineering...")
    
    # Sort by country and year for lag features
    df = df.sort_values(['country_code', 'year']).reset_index(drop=True)
    
    # 1. Time-based features
    df['year_normalized'] = (df['year'] - df['year'].min()) / (df['year'].max() - df['year'].min())
    df['year_sin'] = np.sin(2 * np.pi * df['year_normalized'])
    df['year_cos'] = np.cos(2 * np.pi * df['year_normalized'])
    
    # 2. Lag features (previous year values)
    df['gdp_lag_1'] = df.groupby('country_code')['gdp_value'].shift(1)
    df['gdp_lag_2'] = df.groupby('country_code')['gdp_value'].shift(2)
    df['gdp_growth_lag_1'] = df.groupby('country_code')['gdp_growth_rate'].shift(1)
    df['ranking_lag_1'] = df.groupby('country_code')['ranking'].shift(1)
    
    # 3. Rolling statistics (3-year window)
    df['gdp_rolling_mean_3'] = df.groupby('country_code')['gdp_value'].rolling(window=3, min_periods=1).mean().reset_index(drop=True)
    df['gdp_rolling_std_3'] = df.groupby('country_code')['gdp_value'].rolling(window=3, min_periods=1).std().reset_index(drop=True)
    df['growth_rolling_mean_3'] = df.groupby('country_code')['gdp_growth_rate'].rolling(window=3, min_periods=1).mean().reset_index(drop=True)
    
    # 4. Change features
    df['gdp_change'] = df.groupby('country_code')['gdp_value'].diff()
    df['ranking_change'] = df.groupby('country_code')['ranking'].diff()
    df['gdp_pct_change'] = df.groupby('country_code')['gdp_value'].pct_change()
    
    # 5. Relative features
    df['gdp_vs_year_mean'] = df['gdp_value'] / df.groupby('year')['gdp_value'].transform('mean')
    df['ranking_percentile'] = df.groupby('year')['ranking'].rank(pct=True)
    df['gdp_percentile'] = df.groupby('year')['gdp_value'].rank(pct=True)
    
    # 6. Economic indicators
    df['gdp_per_capita_proxy'] = df['gdp_billions'] / (df['ranking'] + 1)  # Inverse ranking as population proxy
    df['economic_stability'] = 1 / (abs(df['gdp_growth_rate']) + 1)
    df['growth_momentum'] = df['gdp_growth_rate'] * df['gdp_vs_year_mean']
    
    # 7. Local data interactions
    df['gdp_local_fob_ratio'] = np.where(df['local_avg_fob'] > 0, 
                                        df['gdp_billions'] / df['local_avg_fob'], 0)
    df['gdp_export_diversity'] = df['gdp_billions'] * df['local_unique_destinations']
    df['export_intensity'] = df['local_export_records'] / (df['gdp_billions'] + 1)
    
    # 8. Interaction features
    df['gdp_growth_ranking_interaction'] = df['gdp_growth_rate'] * (1 / (df['ranking'] + 1))
    df['region_gdp_interaction'] = df.groupby('region')['gdp_value'].transform('mean')
    
    # 9. Volatility measures
    df['gdp_volatility'] = df.groupby('country_code')['gdp_value'].rolling(window=3, min_periods=1).std().reset_index(drop=True)
    df['growth_volatility'] = df.groupby('country_code')['gdp_growth_rate'].rolling(window=3, min_periods=1).std().reset_index(drop=True)
    
    # Fill NaN values
    df['gdp_rolling_std_3'] = df['gdp_rolling_std_3'].fillna(0)
    df['gdp_volatility'] = df['gdp_volatility'].fillna(0)
    df['growth_volatility'] = df['growth_volatility'].fillna(0)
    
    logging.info(f"Feature engineering completed. Features created: {len(df.columns)}")
    logging.info(f"Sample of new features: {[col for col in df.columns if 'lag' in col or 'rolling' in col or 'change' in col][:10]}")
    
    return df

def prepare_ml_dataset(**context):
    """Prepare dataset for ML training"""
    df = context['task_instance'].xcom_pull(task_ids='extract_training_data')
    
    if df is None or df.empty:
        raise ValueError("No data received from extraction task")
    
    # Apply feature engineering
    df_features = feature_engineering(df)
    
    # Encode categorical variables
    label_encoders = {}
    
    # Encode country
    le_country = LabelEncoder()
    df_features['country_code_encoded'] = le_country.fit_transform(df_features['country_code'])
    label_encoders['country_code'] = le_country
    
    # Encode region
    le_region = LabelEncoder()
    df_features['region_encoded'] = le_region.fit_transform(df_features['region'])
    label_encoders['region'] = le_region
    
    # Encode economy size
    le_economy = LabelEncoder()
    df_features['economy_size_encoded'] = le_economy.fit_transform(df_features['economy_size'])
    label_encoders['economy_size'] = le_economy
    
    # Define feature columns for training
    feature_columns = [
        # Basic features
        'year', 'ranking', 'gdp_growth_rate', 'log_gdp',
        
        # Local data features
        'local_avg_fob', 'local_avg_weight', 'local_export_records',
        'local_unique_destinations', 'local_unique_products', 'local_unique_regions',
        
        # Engineered features
        'year_sin', 'year_cos', 'gdp_lag_1', 'gdp_lag_2', 'gdp_growth_lag_1', 'ranking_lag_1',
        'gdp_rolling_mean_3', 'gdp_rolling_std_3', 'growth_rolling_mean_3',
        'gdp_change', 'ranking_change', 'gdp_pct_change',
        'gdp_vs_year_mean', 'ranking_percentile', 'gdp_percentile',
        'gdp_per_capita_proxy', 'economic_stability', 'growth_momentum',
        'gdp_local_fob_ratio', 'gdp_export_diversity', 'export_intensity',
        'gdp_growth_ranking_interaction', 'region_gdp_interaction',
        'gdp_volatility', 'growth_volatility',
        
        # Encoded categoricals
        'country_code_encoded', 'region_encoded', 'economy_size_encoded'
    ]
    
    # Filter to available columns
    available_features = [col for col in feature_columns if col in df_features.columns]
    
    # Remove rows with missing lag features (can't predict without historical data)
    df_clean = df_features.dropna(subset=['gdp_lag_1']).copy()
    
    if df_clean.empty:
        raise ValueError("No data available after removing missing lag features")
    
    logging.info(f"Prepared dataset with {len(df_clean)} samples and {len(available_features)} features")
    logging.info(f"Target variable (GDP) range: ${df_clean['gdp_billions'].min():.2f}B - ${df_clean['gdp_billions'].max():.2f}B")
    
    # Upload processed dataset to MinIO
    minio_client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    bucket_name = 'etl-data'
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    
    # Save dataset
    csv_string = df_clean.to_csv(index=False)
    dataset_object_name = f"ml_datasets/training_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    minio_client.put_object(
        bucket_name,
        dataset_object_name,
        data=BytesIO(csv_string.encode('utf-8')),
        length=len(csv_string.encode('utf-8')),
        content_type='text/csv'
    )
    
    logging.info(f"Training dataset uploaded to MinIO: {dataset_object_name}")
    
    return {
        'dataset_path': dataset_object_name,
        'feature_columns': available_features,
        'label_encoders': label_encoders,
        'sample_count': len(df_clean),
        'feature_count': len(available_features)
    }

def train_gbr_model(**context):
    """Train Gradient Boosting Regressor model"""
    dataset_info = context['task_instance'].xcom_pull(task_ids='prepare_ml_dataset')
    
    # Load dataset from MinIO
    minio_client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    bucket_name = 'etl-data'
    response = minio_client.get_object(bucket_name, dataset_info['dataset_path'])
    df = pd.read_csv(response)
    
    logging.info(f"Loaded training dataset: {len(df)} samples")
    
    # Prepare features and target
    feature_columns = dataset_info['feature_columns']
    X = df[feature_columns].fillna(0)  # Fill any remaining NaN
    y = df['gdp_billions']  # Target: GDP in billions
    
    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=None
    )
    
    logging.info(f"Training set: {len(X_train)} samples")
    logging.info(f"Test set: {len(X_test)} samples")
    
    # Feature scaling
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Hyperparameter tuning with GridSearch
    logging.info("Starting hyperparameter tuning...")
    
    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [6, 8, 10],
        'learning_rate': [0.05, 0.1, 0.15],
        'subsample': [0.8, 0.9, 1.0],
        'min_samples_split': [10, 20],
        'min_samples_leaf': [5, 10]
    }
    
    gbr_base = GradientBoostingRegressor(random_state=42)
    
    # Use smaller parameter grid for faster training in production
    grid_search = GridSearchCV(
        gbr_base, 
        param_grid, 
        cv=3, 
        scoring='r2', 
        n_jobs=-1, 
        verbose=1
    )
    
    grid_search.fit(X_train_scaled, y_train)
    
    # Best model
    best_gbr = grid_search.best_estimator_
    
    logging.info(f"Best parameters: {grid_search.best_params_}")
    logging.info(f"Best CV score: {grid_search.best_score_:.4f}")
    
    # Final predictions
    y_train_pred = best_gbr.predict(X_train_scaled)
    y_test_pred = best_gbr.predict(X_test_scaled)
    
    # Model evaluation
    train_r2 = r2_score(y_train, y_train_pred)
    test_r2 = r2_score(y_test, y_test_pred)
    train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
    test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
    train_mae = mean_absolute_error(y_train, y_train_pred)
    test_mae = mean_absolute_error(y_test, y_test_pred)
    
    logging.info("=== MODEL PERFORMANCE ===")
    logging.info(f"Training R²: {train_r2:.4f}")
    logging.info(f"Test R²: {test_r2:.4f}")
    logging.info(f"Training RMSE: {train_rmse:.4f}")
    logging.info(f"Test RMSE: {test_rmse:.4f}")
    logging.info(f"Training MAE: {train_mae:.4f}")
    logging.info(f"Test MAE: {test_mae:.4f}")
    
    # Feature importance
    feature_importance = []
    for feature, importance in zip(feature_columns, best_gbr.feature_importances_):
        feature_importance.append({
            'feature': feature,
            'importance': float(importance)
        })
    
    feature_importance = sorted(feature_importance, key=lambda x: x['importance'], reverse=True)
    
    logging.info("Top 10 Feature Importances:")
    for i, item in enumerate(feature_importance[:10]):
        logging.info(f"  {i+1}. {item['feature']}: {item['importance']:.4f}")
    
    # Cross-validation
    cv_scores = cross_val_score(best_gbr, X_train_scaled, y_train, cv=5, scoring='r2')
    
    logging.info(f"Cross-validation R² scores: {cv_scores}")
    logging.info(f"Mean CV R²: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
    
    model_version = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    return {
        'model': best_gbr,
        'scaler': scaler,
        'label_encoders': dataset_info['label_encoders'],
        'feature_columns': feature_columns,
        'feature_importance': feature_importance,
        'model_version': model_version,
        'performance_metrics': {
            'train_r2': train_r2,
            'test_r2': test_r2,
            'train_rmse': train_rmse,
            'test_rmse': test_rmse,
            'train_mae': train_mae,
            'test_mae': test_mae,
            'cv_mean': cv_scores.mean(),
            'cv_std': cv_scores.std(),
            'best_params': grid_search.best_params_
        },
        'training_samples': len(X_train),
        'test_samples': len(X_test)
    }

def save_model_to_minio(**context):
    """Save trained model and components to MinIO"""
    model_data = context['task_instance'].xcom_pull(task_ids='train_gbr_model')
    
    minio_client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    bucket_name = 'etl-data'
    model_version = model_data['model_version']
    
    try:
        # Save main model
        model_buffer = BytesIO()
        joblib.dump(model_data['model'], model_buffer)
        model_buffer.seek(0)
        
        model_object_name = f"models/gbr_model_{model_version}.joblib"
        minio_client.put_object(
            bucket_name,
            model_object_name,
            data=model_buffer,
            length=model_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        
        logging.info(f"Model saved to MinIO: {model_object_name}")
        
        # Save scaler
        scaler_buffer = BytesIO()
        joblib.dump(model_data['scaler'], scaler_buffer)
        scaler_buffer.seek(0)
        
        scaler_object_name = f"models/scaler_{model_version}.joblib"
        minio_client.put_object(
            bucket_name,
            scaler_object_name,
            data=scaler_buffer,
            length=scaler_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        
        logging.info(f"Scaler saved to MinIO: {scaler_object_name}")
        
        # Save label encoders
        encoders_buffer = BytesIO()
        pickle.dump(model_data['label_encoders'], encoders_buffer)
        encoders_buffer.seek(0)
        
        encoders_object_name = f"models/label_encoders_{model_version}.pkl"
        minio_client.put_object(
            bucket_name,
            encoders_object_name,
            data=encoders_buffer,
            length=encoders_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        
        logging.info(f"Label encoders saved to MinIO: {encoders_object_name}")
        
        # Save feature importance as JSON
        importance_json = json.dumps(model_data['feature_importance'], indent=2)
        importance_object_name = f"models/feature_importance_{model_version}.json"
        
        minio_client.put_object(
            bucket_name,
            importance_object_name,
            data=BytesIO(importance_json.encode('utf-8')),
            length=len(importance_json.encode('utf-8')),
            content_type='application/json'
        )
        
        logging.info(f"Feature importance saved to MinIO: {importance_object_name}")
        
        # Save model metadata as JSON
        metadata = {
            'model_version': model_version,
            'model_type': 'gradient_boosting_regressor',
            'training_date': datetime.now().isoformat(),
            'feature_columns': model_data['feature_columns'],
            'performance_metrics': model_data['performance_metrics'],
            'training_samples': model_data['training_samples'],
            'test_samples': model_data['test_samples'],
            'model_path': model_object_name,
            'scaler_path': scaler_object_name,
            'encoders_path': encoders_object_name,
            'importance_path': importance_object_name
        }
        
        metadata_json = json.dumps(metadata, indent=2)
        metadata_object_name = f"models/model_metadata_{model_version}.json"
        
        minio_client.put_object(
            bucket_name,
            metadata_object_name,
            data=BytesIO(metadata_json.encode('utf-8')),
            length=len(metadata_json.encode('utf-8')),
            content_type='application/json'
        )
        
        logging.info(f"Model metadata saved to MinIO: {metadata_object_name}")
        
        return {
            'model_version': model_version,
            'model_path': model_object_name,
            'metadata_path': metadata_object_name,
            'performance_metrics': model_data['performance_metrics']
        }
        
    except Exception as e:
        logging.error(f"Error saving model to MinIO: {str(e)}")
        raise

def update_model_registry(**context):
    """Update model registry in PostgreSQL"""
    model_info = context['task_instance'].xcom_pull(task_ids='save_model_to_minio')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Deactivate previous models
        deactivate_query = """
        UPDATE etl_schema.model_metadata 
        SET is_active = FALSE 
        WHERE model_type = 'gdp_regression'
        """
        postgres_hook.run(deactivate_query)
        
        # Insert new model
        insert_query = """
        INSERT INTO etl_schema.model_metadata (
            model_name, model_version, model_type, training_date,
            accuracy_score, feature_names, model_path, data_period,
            countries_count, training_samples, test_samples, is_active
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        feature_names_json = json.dumps(model_info['performance_metrics'].get('feature_columns', []))
        
        postgres_hook.run(insert_query, parameters=(
            'GDP_Prediction_GBR',
            model_info['model_version'],
            'gdp_regression',
            datetime.now(),
            model_info['performance_metrics']['test_r2'],
            feature_names_json,
            model_info['model_path'],
            '2019-2024',
            0,  # Will be updated with actual count
            model_info['performance_metrics'].get('training_samples', 0),
            model_info['performance_metrics'].get('test_samples', 0),
            True
        ))
        
        logging.info(f"Model registered in database with version: {model_info['model_version']}")
        logging.info(f"Model performance (R²): {model_info['performance_metrics']['test_r2']:.4f}")
        
        return model_info['model_version']
        
    except Exception as e:
        logging.error(f"Error updating model registry: {str(e)}")
        raise

def validate_model_deployment(**context):
    """Validate that the model can be loaded and used for predictions"""
    model_version = context['task_instance'].xcom_pull(task_ids='update_model_registry')
    
    minio_client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    bucket_name = 'etl-data'
    
    try:
        # Test loading the model
        model_object_name = f"models/gbr_model_{model_version}.joblib"
        model_data = minio_client.get_object(bucket_name, model_object_name)
        model = joblib.load(BytesIO(model_data.read()))
        
        # Test loading the scaler
        scaler_object_name = f"models/scaler_{model_version}.joblib"
        scaler_data = minio_client.get_object(bucket_name, scaler_object_name)
        scaler = joblib.load(BytesIO(scaler_data.read()))
        
        # Test loading encoders
        encoders_object_name = f"models/label_encoders_{model_version}.pkl"
        encoders_data = minio_client.get_object(bucket_name, encoders_object_name)
        encoders = pickle.load(BytesIO(encoders_data.read()))
        
        logging.info("✅ Model components loaded successfully")
        logging.info(f"✅ Model type: {type(model).__name__}")
        logging.info(f"✅ Scaler type: {type(scaler).__name__}")
        logging.info(f"✅ Encoders available: {list(encoders.keys())}")
        
        # Basic validation with dummy data
        n_features = len(model.feature_importances_)
        dummy_features = np.random.random((1, n_features))
        dummy_scaled = scaler.transform(dummy_features)
        prediction = model.predict(dummy_scaled)
        
        logging.info(f"✅ Test prediction successful: {prediction[0]:.2f}")
        
        validation_results = {
            'model_version': model_version,
            'validation_status': 'PASSED',
            'model_loaded': True,
            'scaler_loaded': True,
            'encoders_loaded': True,
            'prediction_test': True,
            'test_prediction_value': float(prediction[0])
        }
        
        logging.info("🎉 Model deployment validation PASSED!")
        
        return validation_results
        
    except Exception as e:
        logging.error(f"❌ Model deployment validation FAILED: {str(e)}")
        raise

# Define tasks
extract_data_task = PythonOperator(
    task_id='extract_training_data',
    python_callable=extract_training_data,
    dag=dag
)

prepare_dataset_task = PythonOperator(
    task_id='prepare_ml_dataset',
    python_callable=prepare_ml_dataset,
    dag=dag
)

train_model_task = PythonOperator(
    task_id='train_gbr_model',
    python_callable=train_gbr_model,
    dag=dag
)

save_model_task = PythonOperator(
    task_id='save_model_to_minio',
    python_callable=save_model_to_minio,
    dag=dag
)

update_registry_task = PythonOperator(
    task_id='update_model_registry',
    python_callable=update_model_registry,
    dag=dag
)

validate_deployment_task = PythonOperator(
    task_id='validate_model_deployment',
    python_callable=validate_model_deployment,
    dag=dag
)

# Task dependencies
extract_data_task >> prepare_dataset_task >> train_model_task >> save_model_task >> update_registry_task >> validate_deployment_task