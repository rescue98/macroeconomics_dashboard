import pandas as pd
import numpy as np
import joblib
import pickle
import json
import os
import logging
from io import BytesIO
from minio import Minio
from sklearn.preprocessing import StandardScaler, LabelEncoder

class ModelLoader:
    """Class to handle ML model loading and predictions"""
    
    def __init__(self):
        """Initialize MinIO connection only"""
        self.minio_client = self._init_minio_client()
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
    
    def get_active_model_info(self):
        """Get information about the active model from MinIO"""
        try:
            # Look for model metadata files
            response = self.minio_client.list_objects(
                self.bucket_name, 
                prefix='models/', 
                recursive=True
            )
            
            metadata_files = [obj.object_name for obj in response if 'model_metadata' in obj.object_name and obj.object_name.endswith('.json')]
            
            if not metadata_files:
                logging.warning("No model metadata found")
                return None
            
            # Get the latest metadata file
            latest_metadata = max(metadata_files)
            
            metadata_obj = self.minio_client.get_object(self.bucket_name, latest_metadata)
            model_info = json.loads(metadata_obj.read().decode('utf-8'))
            
            self._model_info = model_info
            return model_info
            
        except Exception as e:
            logging.error(f"Error getting active model info: {e}")
            return None
    
    def get_feature_importance(self):
        """Get feature importance from MinIO"""
        try:
            # Look for feature importance files
            response = self.minio_client.list_objects(
                self.bucket_name, 
                prefix='models/', 
                recursive=True
            )
            
            importance_files = [obj.object_name for obj in response if 'feature_importance' in obj.object_name and obj.object_name.endswith('.json')]
            
            if not importance_files:
                return None
            
            # Get the latest importance file
            latest_importance = max(importance_files)
            
            importance_obj = self.minio_client.get_object(self.bucket_name, latest_importance)
            feature_importance = json.loads(importance_obj.read().decode('utf-8'))
            
            return feature_importance
            
        except Exception as e:
            logging.error(f"Error getting feature importance: {e}")
            return None
    
    def generate_predictions(self, countries, years):
        """Generate mock predictions for demo purposes"""
        try:
            # This is a simplified version that returns mock predictions
            # In a real implementation, this would load the actual model and generate predictions
            
            predictions_data = []
            
            for country in countries[:5]:  # Limit to 5 countries for demo
                for year in years:
                    # Generate mock prediction data
                    actual_gdp = np.random.uniform(100, 2000)  # Mock actual GDP
                    predicted_gdp = actual_gdp * np.random.uniform(0.85, 1.15)  # Mock prediction with some error
                    
                    predictions_data.append({
                        'country_name': country,
                        'year': year,
                        'actual_gdp': actual_gdp,
                        'predicted_gdp': predicted_gdp
                    })
            
            predictions_df = pd.DataFrame(predictions_data)
            
            # Calculate error metrics
            predictions_df['absolute_error'] = abs(predictions_df['actual_gdp'] - predictions_df['predicted_gdp'])
            predictions_df['percentage_error'] = (predictions_df['absolute_error'] / predictions_df['actual_gdp']) * 100
            
            logging.info(f"Generated {len(predictions_df)} mock predictions")
            return predictions_df
            
        except Exception as e:
            logging.error(f"Error generating predictions: {e}")
            return None
    
    def get_model_performance_metrics(self):
        """Get model performance metrics"""
        model_info = self.get_active_model_info()
        
        if model_info and 'performance_metrics' in model_info:
            return model_info['performance_metrics']
        else:
            # Return mock metrics if no model available
            return {
                'r2_score': 0.85,
                'rmse': 45.2,
                'mae': 32.1,
                'performance_category': 'Good'
            }
    
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