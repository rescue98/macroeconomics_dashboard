import pandas as pd
import numpy as np
import joblib
import pickle
import json
import os
import logging
from io import BytesIO
from minio import Minio
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler, LabelEncoder

class ModelLoader:
    """Class to handle ML model loading and predictions"""
    
    def __init__(self):
        """Initialize MinIO and database connections"""
        self.minio_client = self._init_minio_client()
        self.engine = self._init_db_engine()
        self.bucket_name = 'etl-data'
        
        # Cached model components
        self._model = None
        self._scaler = None
        self._label_encoders = None
        self._feature_columns = None
        self._model_info = None
        
    def _init_minio_client(self):
        """Initialize MinIO client"""
        try:
            return Minio(
                endpoint=os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
                access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
                secure=False
            )
        except Exception as e:
            logging.error(f"Error initializing MinIO client: {e}")
            return None
    
    def _init_db_engine(self):
        """Initialize database engine"""
        try:
            db_config = {
                'host': os.getenv('POSTGRES_HOST', 'localhost'),
                'database': os.getenv('POSTGRES_DB', 'etl_database'),
                'user': os.getenv('POSTGRES_USER', 'etl_user'),
                'password': os.getenv('POSTGRES_PASSWORD', 'etl_password'),
                'port': os.getenv('POSTGRES_PORT', '5432')
            }
            
            connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            return create_engine(connection_string)
        except Exception as e:
            logging.error(f"Error initializing database engine: {e}")
            return None
    
    def get_active_model_info(self):
        """Get information about the active model"""
        if self._model_info:
            return self._model_info
            
        query = """
        SELECT 
            model_name,
            model_version,
            model_type,
            training_date,
            accuracy_score,
            feature_names,
            model_path
        FROM etl_schema.model_metadata
        WHERE is_active = true
        ORDER BY training_date DESC
        LIMIT 1
        """
        
        try:
            df = pd.read_sql(query, self.engine)
            if not df.empty:
                model_info = df.iloc[0].to_dict()
                model_info['feature_names'] = json.loads(model_info['feature_names']) if model_info['feature_names'] else []
                self._model_info = model_info
                return model_info
            else:
                logging.warning("No active model found")
                return None
        except Exception as e:
            logging.error(f"Error getting active model info: {e}")
            return None
    
    def load_model_components(self):
        """Load all model components from MinIO"""
        model_info = self.get_active_model_info()
        
        if not model_info:
            logging.error("No active model found")
            return False
        
        model_version = model_info['model_version']
        
        try:
            # Load main model
            if self._model is None:
                model_object_name = f"models/gbr_model_{model_version}.joblib"
                model_data = self.minio_client.get_object(self.bucket_name, model_object_name)
                self._model = joblib.load(BytesIO(model_data.read()))
                logging.info("Model loaded successfully")
            
            # Load scaler
            if self._scaler is None:
                scaler_object_name = f"models/scaler_{model_version}.joblib"
                scaler_data = self.minio_client.get_object(self.bucket_name, scaler_object_name)
                self._scaler = joblib.load(BytesIO(scaler_data.read()))
                logging.info("Scaler loaded successfully")
            
            # Load label encoders
            if self._label_encoders is None:
                encoders_object_name = f"models/label_encoders_{model_version}.pkl"
                encoders_data = self.minio_client.get_object(self.bucket_name, encoders_object_name)
                self._label_encoders = pickle.load(BytesIO(encoders_data.read()))
                logging.info("Label encoders loaded successfully")
            
            # Set feature columns
            if self._feature_columns is None:
                self._feature_columns = model_info['feature_names']
            
            return True
            
        except Exception as e:
            logging.error(f"Error loading model components: {e}")
            return False
    
    def get_feature_importance(self):
        """Get feature importance from the model"""
        model_info = self.get_active_model_info()
        
        if not model_info:
            return None
        
        model_version = model_info['model_version']
        
        try:
            importance_object_name = f"models/feature_importance_{model_version}.json"
            importance_data = self.minio_client.get_object(self.bucket_name, importance_object_name)
            feature_importance = json.loads(importance_data.read().decode('utf-8'))
            return feature_importance
        except Exception as e:
            logging.error(f"Error getting feature importance: {e}")
            return None
    
    def prepare_prediction_features(self, countries, years):
        """Prepare features for prediction"""
        try:
            # Get base data for features
            years_str = ','.join(map(str, years))
            countries_str = "','".join(countries)
            
            query = f"""
            WITH local_data_agg AS (
                SELECT 
                    year,
                    AVG(value_1) as avg_value_1,
                    AVG(value_2) as avg_value_2,
                    COUNT(*) as local_record_count
                FROM etl_schema.local_data
                GROUP BY year
            ),
            worldbank_data AS (
                SELECT 
                    country_code,
                    country_name,
                    year,
                    gdp_value,
                    ranking,
                    gdp_growth_rate,
                    COALESCE(gdp_growth_rate, 0) as gdp_growth_rate_filled
                FROM etl_schema.world_bank_gdp
                WHERE gdp_value IS NOT NULL
                AND year IN ({years_str})
                AND country_name IN ('{countries_str}')
            )
            SELECT 
                wb.country_code,
                wb.country_name,
                wb.year,
                wb.gdp_value,
                wb.ranking,
                wb.gdp_growth_rate_filled as gdp_growth_rate,
                COALESCE(ld.avg_value_1, 0) as local_avg_value_1,
                COALESCE(ld.avg_value_2, 0) as local_avg_value_2,
                COALESCE(ld.local_record_count, 0) as local_record_count,
                LOG(wb.gdp_value) as log_gdp,
                wb.gdp_value / 1000000000.0 as gdp_billions
            FROM worldbank_data wb
            LEFT JOIN local_data_agg ld ON wb.year = ld.year
            ORDER BY wb.year, wb.ranking
            """
            
            df = pd.read_sql(query, self.engine)
            
            if df.empty:
                logging.warning("No data available for prediction")
                return None
            
            # Feature engineering (same as in training)
            df = df.sort_values(['country_code', 'year'])
            
            # GDP lag features
            df['gdp_lag_1'] = df.groupby('country_code')['gdp_value'].shift(1)
            df['gdp_growth_lag_1'] = df.groupby('country_code')['gdp_growth_rate'].shift(1)
            
            # Ranking features
            df['ranking_change'] = df.groupby('country_code')['ranking'].diff()
            df['ranking_percentile'] = df['ranking'].rank(pct=True)
            
            # Economic indicators
            df['gdp_per_capita_proxy'] = df['gdp_billions'] / (df['ranking'] + 1)
            
            # Interaction features
            df['gdp_local_interaction'] = df['gdp_billions'] * df['local_avg_value_1']
            df['growth_ranking_interaction'] = df['gdp_growth_rate'] * (1 / (df['ranking'] + 1))
            
            # Cyclical time features
            df['year_sin'] = np.sin(2 * np.pi * (df['year'] - df['year'].min()) / (df['year'].max() - df['year'].min() + 1))
            df['year_cos'] = np.cos(2 * np.pi * (df['year'] - df['year'].min()) / (df['year'].max() - df['year'].min() + 1))
            
            # Encode categorical variables
            if 'country_code' in self._label_encoders:
                # Handle unseen countries
                known_countries = set(self._label_encoders['country_code'].classes_)
                df['country_code_encoded'] = df['country_code'].apply(
                    lambda x: self._label_encoders['country_code'].transform([x])[0] 
                    if x in known_countries else -1
                )
            else:
                df['country_code_encoded'] = 0
            
            # Fill missing values
            df = df.fillna(method='ffill').fillna(method='bfill').fillna(0)
            
            return df
            
        except Exception as e:
            logging.error(f"Error preparing prediction features: {e}")
            return None
    
    def generate_predictions(self, countries, years):
        """Generate predictions for specified countries and years"""
        # Load model components if not already loaded
        if not self.load_model_components():
            logging.error("Failed to load model components")
            return None
        
        # Prepare features
        df = self.prepare_prediction_features(countries, years)
        
        if df is None or df.empty:
            logging.error("No data available for prediction")
            return None
        
        try:
            # Select feature columns for prediction
            feature_columns = [
                'year', 'ranking', 'gdp_growth_rate', 'local_avg_value_1', 'local_avg_value_2',
                'local_record_count', 'log_gdp', 'gdp_lag_1', 'gdp_growth_lag_1',
                'ranking_change', 'ranking_percentile', 'gdp_per_capita_proxy',
                'gdp_local_interaction', 'growth_ranking_interaction',
                'year_sin', 'year_cos', 'country_code_encoded'
            ]
            
            # Filter out rows with missing lag features for prediction
            df_pred = df.dropna(subset=['gdp_lag_1']).copy()
            
            if df_pred.empty:
                logging.warning("No data available after removing missing lag features")
                return None
            
            X = df_pred[feature_columns]
            
            # Scale features
            X_scaled = self._scaler.transform(X)
            
            # Generate predictions
            predictions = self._model.predict(X_scaled)
            
            # Prepare results DataFrame
            results = df_pred[['country_code', 'country_name', 'year', 'gdp_billions']].copy()
            results['predicted_gdp'] = predictions
            results['actual_gdp'] = results['gdp_billions']
            
            # Calculate prediction metrics
            results['absolute_error'] = abs(results['actual_gdp'] - results['predicted_gdp'])
            results['percentage_error'] = (results['absolute_error'] / results['actual_gdp']) * 100
            
            logging.info(f"Generated {len(results)} predictions")
            return results
            
        except Exception as e:
            logging.error(f"Error generating predictions: {e}")
            return None
    
    def predict_future_gdp(self, countries, future_years):
        """Predict GDP for future years (experimental)"""
        # This would require more sophisticated time series modeling
        # For now, return a placeholder
        logging.warning("Future GDP prediction not yet implemented")
        return None
    
    def get_model_performance_metrics(self):
        """Get model performance metrics from database"""
        query = """
        SELECT 
            model_name,
            model_version,
            accuracy_score,
            training_date,
            CASE 
                WHEN accuracy_score >= 0.8 THEN 'Excellent'
                WHEN accuracy_score >= 0.6 THEN 'Good'
                WHEN accuracy_score >= 0.4 THEN 'Fair'
                ELSE 'Poor'
            END as performance_category
        FROM etl_schema.model_metadata
        WHERE is_active = true
        """
        
        try:
            df = pd.read_sql(query, self.engine)
            return df.iloc[0].to_dict() if not df.empty else {}
        except Exception as e:
            logging.error(f"Error getting model performance metrics: {e}")
            return {}
    
    def validate_predictions(self, predictions_df):
        """Validate prediction results"""
        if predictions_df is None or predictions_df.empty:
            return False, "No predictions to validate"
        
        validation_results = {
            'total_predictions': len(predictions_df),
            'mean_absolute_error': predictions_df['absolute_error'].mean(),
            'median_absolute_error': predictions_df['absolute_error'].median(),
            'mean_percentage_error': predictions_df['percentage_error'].mean(),
            'predictions_within_10_percent': len(predictions_df[predictions_df['percentage_error'] <= 10]),
            'predictions_within_20_percent': len(predictions_df[predictions_df['percentage_error'] <= 20])
        }
        
        # Check if predictions are reasonable
        is_valid = (
            validation_results['mean_percentage_error'] < 50 and  # Average error less than 50%
            validation_results['predictions_within_20_percent'] > len(predictions_df) * 0.5  # At least 50% within 20%
        )
        
        return is_valid, validation_results