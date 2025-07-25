import pandas as pd
import boto3
import os
import logging
from datetime import datetime, timedelta
from io import StringIO

class DataQueries:
    """Class to handle all data queries from MinIO for the Streamlit app"""
    
    def __init__(self):
        """Initialize MinIO S3 client"""
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
        )
        self.bucket_name = 'etl-data'
        
    def get_available_years(self):
        """Get list of available years from processed data"""
        try:
            # Try World Bank data first
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_latest_file('processed/worldbank_data/')
            )
            df = pd.read_csv(obj['Body'])
            
            if 'year' in df.columns:
                return sorted(df['year'].dropna().unique().tolist(), reverse=True)
            
            return []
        except Exception as e:
            logging.error(f"Error getting available years: {e}")
            return []
    
    def get_available_countries(self):
        """Get list of available countries from World Bank data"""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_latest_file('processed/worldbank_data/')
            )
            df = pd.read_csv(obj['Body'])
            
            if 'country_name' in df.columns:
                return sorted(df['country_name'].dropna().unique().tolist())
            
            return []
        except Exception as e:
            logging.error(f"Error getting available countries: {e}")
            return []
    
    def get_worldbank_overview(self, years):
        """Get World Bank overview from processed data"""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_latest_file('processed/worldbank_data/')
            )
            df = pd.read_csv(obj['Body'])
            
            if years:
                df = df[df['year'].isin(years)]
            
            return {
                'total_records': len(df),
                'total_countries': df['country_code'].nunique() if 'country_code' in df.columns else 0,
                'avg_gdp': df['gdp_value'].mean() if 'gdp_value' in df.columns else 0,
                'max_gdp': df['gdp_value'].max() if 'gdp_value' in df.columns else 0,
                'min_gdp': df['gdp_value'].min() if 'gdp_value' in df.columns else 0
            }
        except Exception as e:
            logging.error(f"Error getting World Bank overview: {e}")
            return {}
    
    def get_local_data_overview(self, years):
        """Get local export data overview"""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_latest_file('processed/local_data/')
            )
            df = pd.read_csv(obj['Body'])
            
            if years and 'YEAR' in df.columns:
                df = df[df['YEAR'].isin(years)]
            
            fob_col = 'FOB_CLEAN' if 'FOB_CLEAN' in df.columns else 'US$ FOB'
            
            return {
                'total_records': len(df),
                'total_countries': df['PAIS_DESTINO_CLEAN'].nunique() if 'PAIS_DESTINO_CLEAN' in df.columns else 0,
                'total_fob_usd': df[fob_col].sum() if fob_col in df.columns else 0,
                'avg_fob_usd': df[fob_col].mean() if fob_col in df.columns else 0
            }
        except Exception as e:
            logging.error(f"Error getting local data overview: {e}")
            return {}
    
    def get_gdp_by_year(self, years):
        """Get GDP distribution by year"""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_latest_file('processed/worldbank_data/')
            )
            df = pd.read_csv(obj['Body'])
            
            if years:
                df = df[df['year'].isin(years)]
            
            required_cols = ['year', 'country_name', 'gdp_value', 'ranking']
            available_cols = [col for col in required_cols if col in df.columns]
            
            return df[available_cols].sort_values(['year', 'ranking'] if 'ranking' in available_cols else ['year'])
        except Exception as e:
            logging.error(f"Error getting GDP by year: {e}")
            return pd.DataFrame()
    
    def get_local_data_trends(self, years):
        """Get local export data trends"""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_latest_file('processed/local_data/')
            )
            df = pd.read_csv(obj['Body'])
            
            if years and 'YEAR' in df.columns:
                df = df[df['YEAR'].isin(years)]
            
            # Create trends from export data
            fob_col = 'FOB_CLEAN' if 'FOB_CLEAN' in df.columns else 'US$ FOB'
            
            if 'YEAR' in df.columns and fob_col in df.columns:
                trends = df.groupby('YEAR').agg({
                    fob_col: ['mean', 'count']
                }).reset_index()
                
                trends.columns = ['year', 'avg_value', 'record_count']
                trends['category'] = 'exports'
                
                return trends
            
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Error getting local data trends: {e}")
            return pd.DataFrame()
    
    def get_recent_updates(self):
        """Get recent data updates info"""
        try:
            updates = []
            
            # Check World Bank data
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix='processed/worldbank_data/'
                )
                if 'Contents' in response:
                    latest_wb = max(response['Contents'], key=lambda x: x['LastModified'])
                    updates.append({
                        'data_source': 'World Bank',
                        'last_update': latest_wb['LastModified'],
                        'record_count': 'Available'
                    })
            except:
                pass
            
            # Check local data
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix='processed/local_data/'
                )
                if 'Contents' in response:
                    latest_local = max(response['Contents'], key=lambda x: x['LastModified'])
                    updates.append({
                        'data_source': 'Local Exports',
                        'last_update': latest_local['LastModified'],
                        'record_count': 'Available'
                    })
            except:
                pass
            
            return pd.DataFrame(updates)
        except Exception as e:
            logging.error(f"Error getting recent updates: {e}")
            return pd.DataFrame()
    
    def get_gdp_rankings(self, years, countries):
        """Get GDP rankings for selected countries and years"""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_latest_file('processed/worldbank_data/')
            )
            df = pd.read_csv(obj['Body'])
            
            if years:
                df = df[df['year'].isin(years)]
            
            if countries and 'country_name' in df.columns:
                df = df[df['country_name'].isin(countries)]
            
            return df.sort_values(['year', 'ranking'] if 'ranking' in df.columns else ['year'])
        except Exception as e:
            logging.error(f"Error getting GDP rankings: {e}")
            return pd.DataFrame()
    
    def get_gdp_growth_analysis(self, years, countries):
        """Get GDP growth analysis"""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_latest_file('processed/worldbank_data/')
            )
            df = pd.read_csv(obj['Body'])
            
            if years:
                df = df[df['year'].isin(years)]
            
            if countries and 'country_name' in df.columns:
                df = df[df['country_name'].isin(countries)]
            
            # Filter for valid growth rates
            if 'gdp_growth_rate' in df.columns:
                df = df[df['gdp_growth_rate'].notna()]
            
            return df.sort_values(['country_name', 'year'])
        except Exception as e:
            logging.error(f"Error getting GDP growth analysis: {e}")
            return pd.DataFrame()
    
    def get_regional_performance(self, years):
        """Get regional performance from World Bank data"""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_latest_file('processed/worldbank_data/')
            )
            df = pd.read_csv(obj['Body'])
            
            if years:
                df = df[df['year'].isin(years)]
            
            # Group by region if available
            if 'region' in df.columns and 'gdp_value' in df.columns:
                regional_data = df.groupby('region').agg({
                    'gdp_value': 'sum',
                    'gdp_growth_rate': 'mean',
                    'country_code': 'count'
                }).reset_index()
                
                regional_data.columns = ['region', 'total_gdp', 'avg_growth', 'country_count']
                return regional_data.sort_values('total_gdp', ascending=False)
            
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Error getting regional performance: {e}")
            return pd.DataFrame()
    
    def get_export_data_summary(self, years):
        """Get export data summary"""
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_latest_file('processed/local_data/')
            )
            df = pd.read_csv(obj['Body'])
            
            if years and 'YEAR' in df.columns:
                df = df[df['YEAR'].isin(years)]
            
            fob_col = 'FOB_CLEAN' if 'FOB_CLEAN' in df.columns else 'US$ FOB'
            
            summary = {
                'total_records': len(df),
                'total_fob_usd': df[fob_col].sum() if fob_col in df.columns else 0,
                'unique_countries': df['PAIS_DESTINO_CLEAN'].nunique() if 'PAIS_DESTINO_CLEAN' in df.columns else 0,
                'unique_products': df['PRODUCTO_CLEAN'].nunique() if 'PRODUCTO_CLEAN' in df.columns else 0,
                'unique_regions': df['REGION_ORIGEN_CLEAN'].nunique() if 'REGION_ORIGEN_CLEAN' in df.columns else 0
            }
            
            return summary
        except Exception as e:
            logging.error(f"Error getting export data summary: {e}")
            return {}
    
    def get_exports_by_country(self, years):
        """Get exports aggregated by destination country"""
        try:
            # Try to get monthly aggregations first
            try:
                obj = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=self._get_latest_file('processed/local_data/', 'monthly_exports_by_country')
                )
                df = pd.read_csv(obj['Body'])
                
                if years and 'YEAR' in df.columns:
                    df = df[df['YEAR'].isin(years)]
                
                return df.groupby('PAIS_DESTINO_CLEAN').agg({
                    'TOTAL_FOB_USD': 'sum',
                    'EXPORT_COUNT': 'sum'
                }).reset_index().sort_values('TOTAL_FOB_USD', ascending=False)
            except:
                # Fallback to main processed data
                obj = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=self._get_latest_file('processed/local_data/')
                )
                df = pd.read_csv(obj['Body'])
                
                if years and 'YEAR' in df.columns:
                    df = df[df['YEAR'].isin(years)]
                
                fob_col = 'FOB_CLEAN' if 'FOB_CLEAN' in df.columns else 'US$ FOB'
                country_col = 'PAIS_DESTINO_CLEAN' if 'PAIS_DESTINO_CLEAN' in df.columns else 'PAIS DE DESTINO'
                
                return df.groupby(country_col).agg({
                    fob_col: ['sum', 'count']
                }).reset_index()
        except Exception as e:
            logging.error(f"Error getting exports by country: {e}")
            return pd.DataFrame()
    
    def _get_latest_file(self, prefix, filename_pattern=None):
        """Get the latest file from a MinIO prefix"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                raise ValueError(f"No files found in {prefix}")
            
            # Filter by pattern if provided
            files = response['Contents']
            if filename_pattern:
                files = [f for f in files if filename_pattern in f['Key']]
            
            # Filter only CSV files
            csv_files = [f for f in files if f['Key'].endswith('.csv')]
            
            if not csv_files:
                raise ValueError(f"No CSV files found in {prefix}")
            
            # Return the most recent file
            latest_file = max(csv_files, key=lambda x: x['LastModified'])
            return latest_file['Key']
            
        except Exception as e:
            logging.error(f"Error getting latest file from {prefix}: {e}")
            raise
    
    def get_data_quality_metrics(self):
        """Get basic data quality metrics from available files"""
        try:
            quality_metrics = {
                'wb_completeness': 0,
                'local_completeness': 0,
                'duplicates': 0,
                'data_age_days': 0
            }
            
            # Check World Bank data quality
            try:
                obj = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=self._get_latest_file('processed/worldbank_data/')
                )
                wb_df = pd.read_csv(obj['Body'])
                
                if 'gdp_value' in wb_df.columns:
                    quality_metrics['wb_completeness'] = (wb_df['gdp_value'].notna().sum() / len(wb_df)) * 100
            except:
                pass
            
            # Check local data quality
            try:
                obj = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=self._get_latest_file('processed/local_data/')
                )
                local_df = pd.read_csv(obj['Body'])
                
                fob_col = 'FOB_CLEAN' if 'FOB_CLEAN' in local_df.columns else 'US$ FOB'
                if fob_col in local_df.columns:
                    quality_metrics['local_completeness'] = (local_df[fob_col].notna().sum() / len(local_df)) * 100
            except:
                pass
            
            return quality_metrics
        except Exception as e:
            logging.error(f"Error getting data quality metrics: {e}")
            return {}
    
    def get_data_quality_issues(self):
        """Get data quality issues"""
        # Return empty DataFrame as we don't have detailed quality checks in MinIO-only setup
        return pd.DataFrame()
    
    def get_data_lineage(self):
        """Get data lineage information"""
        return {
            "data_sources": {
                "local_exports": {
                    "source": "Chilean Export CSV Files",
                    "processing": "Airflow -> Spark -> MinIO",
                    "frequency": "Daily"
                },
                "world_bank": {
                    "source": "World Bank API",
                    "processing": "Airflow -> MinIO",
                    "frequency": "Weekly"
                }
            },
            "storage": "MinIO Object Storage",
            "processing_engine": "Apache Spark",
            "orchestrator": "Apache Airflow"
        }