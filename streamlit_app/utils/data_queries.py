import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import logging
from datetime import datetime, timedelta

class DataQueries:
    """Class to handle all database queries for the Streamlit app"""
    
    def __init__(self):
        """Initialize database connection"""
        self.connection_string = self._get_connection_string()
        self.engine = create_engine(self.connection_string)
        
    def _get_connection_string(self):
        """Get database connection string from environment variables"""
        db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'database': os.getenv('POSTGRES_DB', 'etl_database'),
            'user': os.getenv('POSTGRES_USER', 'etl_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'etl_password'),
            'port': os.getenv('POSTGRES_PORT', '5432')
        }
        
        return f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    
    def get_available_years(self):
        """Get list of available years in the database"""
        query = """
        SELECT DISTINCT year
        FROM etl_schema.world_bank_gdp
        WHERE year IS NOT NULL
        ORDER BY year DESC
        """
        try:
            df = pd.read_sql(query, self.engine)
            return df['year'].tolist()
        except Exception as e:
            logging.error(f"Error getting available years: {e}")
            return []
    
    def get_available_countries(self):
        """Get list of available countries"""
        query = """
        SELECT DISTINCT country_name
        FROM etl_schema.world_bank_gdp
        WHERE country_name IS NOT NULL
        ORDER BY country_name
        """
        try:
            df = pd.read_sql(query, self.engine)
            return df['country_name'].tolist()
        except Exception as e:
            logging.error(f"Error getting available countries: {e}")
            return []
    
    def get_worldbank_overview(self, years):
        """Get World Bank data overview statistics"""
        years_str = ','.join(map(str, years))
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT country_code) as total_countries,
            AVG(gdp_value) as avg_gdp,
            MAX(gdp_value) as max_gdp,
            MIN(gdp_value) as min_gdp
        FROM etl_schema.world_bank_gdp
        WHERE year IN ({years_str})
        AND gdp_value IS NOT NULL
        """
        try:
            df = pd.read_sql(query, self.engine)
            return df.iloc[0].to_dict() if not df.empty else {}
        except Exception as e:
            logging.error(f"Error getting World Bank overview: {e}")
            return {}
    
    def get_local_data_overview(self, years):
        """Get local data overview statistics"""
        years_str = ','.join(map(str, years))
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT category) as total_categories,
            AVG(value_1) as avg_value_1,
            AVG(value_2) as avg_value_2
        FROM etl_schema.local_data
        WHERE year IN ({years_str})
        """
        try:
            df = pd.read_sql(query, self.engine)
            return df.iloc[0].to_dict() if not df.empty else {}
        except Exception as e:
            logging.error(f"Error getting local data overview: {e}")
            return {}
    
    def get_gdp_by_year(self, years):
        """Get GDP distribution by year"""
        years_str = ','.join(map(str, years))
        query = f"""
        SELECT 
            year,
            country_name,
            gdp_value,
            ranking
        FROM etl_schema.world_bank_gdp
        WHERE year IN ({years_str})
        AND gdp_value IS NOT NULL
        ORDER BY year, ranking
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting GDP by year: {e}")
            return pd.DataFrame()
    
    def get_local_data_trends(self, years):
        """Get local data trends"""
        years_str = ','.join(map(str, years))
        query = f"""
        SELECT 
            year,
            category,
            AVG(value_1) as avg_value,
            COUNT(*) as record_count
        FROM etl_schema.local_data
        WHERE year IN ({years_str})
        AND category IS NOT NULL
        GROUP BY year, category
        ORDER BY year, category
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting local data trends: {e}")
            return pd.DataFrame()
    
    def get_recent_updates(self):
        """Get recent data updates"""
        query = """
        SELECT 
            'World Bank' as data_source,
            MAX(created_at) as last_update,
            COUNT(*) as record_count
        FROM etl_schema.world_bank_gdp
        WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
        
        UNION ALL
        
        SELECT 
            'Local Data' as data_source,
            MAX(created_at) as last_update,
            COUNT(*) as record_count
        FROM etl_schema.local_data
        WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting recent updates: {e}")
            return pd.DataFrame()
    
    def get_gdp_rankings(self, years, countries):
        """Get GDP rankings for selected countries and years"""
        years_str = ','.join(map(str, years))
        countries_str = "','".join(countries)
        query = f"""
        SELECT 
            country_code,
            country_name,
            year,
            gdp_value,
            ranking,
            gdp_growth_rate
        FROM etl_schema.world_bank_gdp
        WHERE year IN ({years_str})
        AND country_name IN ('{countries_str}')
        AND gdp_value IS NOT NULL
        ORDER BY year DESC, ranking ASC
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting GDP rankings: {e}")
            return pd.DataFrame()
    
    def get_gdp_growth_analysis(self, years, countries):
        """Get GDP growth analysis"""
        years_str = ','.join(map(str, years))
        countries_str = "','".join(countries)
        query = f"""
        SELECT 
            country_name,
            year,
            gdp_growth_rate,
            gdp_value,
            ranking
        FROM etl_schema.world_bank_gdp
        WHERE year IN ({years_str})
        AND country_name IN ('{countries_str}')
        AND gdp_growth_rate IS NOT NULL
        ORDER BY country_name, year
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting GDP growth analysis: {e}")
            return pd.DataFrame()
    
    def get_regional_performance(self, years):
        """Get regional performance data"""
        years_str = ','.join(map(str, years))
        query = f"""
        WITH regional_data AS (
            SELECT 
                CASE 
                    WHEN country_code IN ('USA', 'CAN', 'MEX') THEN 'North America'
                    WHEN country_code IN ('BRA', 'ARG', 'CHL', 'COL', 'PER') THEN 'South America'
                    WHEN country_code IN ('CHN', 'JPN', 'IND', 'KOR', 'IDN') THEN 'Asia'
                    WHEN country_code IN ('DEU', 'FRA', 'GBR', 'ITA', 'ESP') THEN 'Europe'
                    WHEN country_code IN ('NGA', 'ZAF', 'EGY', 'KEN', 'ETH') THEN 'Africa'
                    ELSE 'Other'
                END as region,
                SUM(gdp_value) as total_gdp,
                AVG(gdp_growth_rate) as avg_growth,
                COUNT(*) as country_count
            FROM etl_schema.world_bank_gdp
            WHERE year IN ({years_str})
            AND gdp_value IS NOT NULL
            GROUP BY region
        )
        SELECT * FROM regional_data
        WHERE region != 'Other'
        ORDER BY total_gdp DESC
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting regional performance: {e}")
            return pd.DataFrame()
    
    def get_local_data_summary(self, years):
        """Get local data summary"""
        years_str = ','.join(map(str, years))
        query = f"""
        SELECT 
            category,
            year,
            COUNT(*) as total_records,
            AVG(value_1) as avg_value_1,
            AVG(value_2) as avg_value_2,
            SUM(value_1 + value_2) as total_value
        FROM etl_schema.local_data
        WHERE year IN ({years_str})
        AND category IS NOT NULL
        GROUP BY category, year
        ORDER BY year, category
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting local data summary: {e}")
            return pd.DataFrame()
    
    def get_local_value_trends(self, years):
        """Get local data value trends"""
        years_str = ','.join(map(str, years))
        query = f"""
        SELECT 
            year,
            AVG(value_1) as avg_value_1,
            AVG(value_2) as avg_value_2,
            COUNT(*) as record_count
        FROM etl_schema.local_data
        WHERE year IN ({years_str})
        GROUP BY year
        ORDER BY year
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting local value trends: {e}")
            return pd.DataFrame()
    
    def get_category_performance(self, years):
        """Get category performance"""
        years_str = ','.join(map(str, years))
        query = f"""
        SELECT 
            category,
            COUNT(*) as record_count,
            SUM(value_1 + value_2) as total_value,
            AVG(value_1) as avg_value_1,
            AVG(value_2) as avg_value_2
        FROM etl_schema.local_data
        WHERE year IN ({years_str})
        AND category IS NOT NULL
        GROUP BY category
        ORDER BY total_value DESC
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting category performance: {e}")
            return pd.DataFrame()
    
    def get_monthly_patterns(self, years):
        """Get monthly patterns in local data"""
        years_str = ','.join(map(str, years))
        query = f"""
        SELECT 
            category,
            month,
            AVG(value_1 + value_2) as avg_value,
            COUNT(*) as record_count
        FROM etl_schema.local_data
        WHERE year IN ({years_str})
        AND month IS NOT NULL
        AND category IS NOT NULL
        GROUP BY category, month
        ORDER BY category, month
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting monthly patterns: {e}")
            return pd.DataFrame()
    
    def get_economic_correlations(self, years, countries):
        """Get economic correlations between World Bank and local data"""
        years_str = ','.join(map(str, years))
        countries_str = "','".join(countries)
        query = f"""
        WITH local_agg AS (
            SELECT 
                year,
                AVG(value_1 + value_2) as local_avg_value,
                COUNT(*) as local_record_count,
                STDDEV(value_1 + value_2) as local_std
            FROM etl_schema.local_data
            WHERE year IN ({years_str})
            GROUP BY year
        ),
        wb_data AS (
            SELECT 
                country_name,
                country_code,
                year,
                gdp_value,
                gdp_growth_rate,
                ranking,
                CASE 
                    WHEN country_code IN ('USA', 'CAN', 'MEX') THEN 'North America'
                    WHEN country_code IN ('BRA', 'ARG', 'CHL', 'COL', 'PER') THEN 'South America'
                    WHEN country_code IN ('CHN', 'JPN', 'IND', 'KOR', 'IDN') THEN 'Asia'
                    WHEN country_code IN ('DEU', 'FRA', 'GBR', 'ITA', 'ESP') THEN 'Europe'
                    ELSE 'Other'
                END as region
            FROM etl_schema.world_bank_gdp
            WHERE year IN ({years_str})
            AND country_name IN ('{countries_str}')
            AND gdp_value IS NOT NULL
        )
        SELECT 
            wb.*,
            la.local_avg_value,
            la.local_record_count,
            (100 - wb.ranking) as local_performance_score
        FROM wb_data wb
        LEFT JOIN local_agg la ON wb.year = la.year
        WHERE la.local_avg_value IS NOT NULL
        ORDER BY wb.year, wb.ranking
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting economic correlations: {e}")
            return pd.DataFrame()
    
    def get_economic_indicators(self, years, countries):
        """Get economic indicators for dashboard"""
        years_str = ','.join(map(str, years))
        countries_str = "','".join(countries)
        query = f"""
        WITH country_stats AS (
            SELECT 
                country_name,
                country_code,
                year,
                gdp_value,
                gdp_growth_rate,
                ranking,
                STDDEV(gdp_growth_rate) OVER (
                    PARTITION BY country_code 
                    ORDER BY year 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as gdp_volatility,
                AVG(gdp_growth_rate) OVER (
                    PARTITION BY country_code 
                    ORDER BY year 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as growth_3yr_avg
            FROM etl_schema.world_bank_gdp
            WHERE year IN ({years_str})
            AND country_name IN ('{countries_str}')
            AND gdp_value IS NOT NULL
        )
        SELECT 
            *,
            CASE 
                WHEN growth_3yr_avg > 3 THEN 'High Growth'
                WHEN growth_3yr_avg > 0 THEN 'Positive Growth'
                WHEN growth_3yr_avg > -2 THEN 'Stable'
                ELSE 'Declining'
            END as growth_category
        FROM country_stats
        ORDER BY country_name, year
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Error getting economic indicators: {e}")
            return pd.DataFrame()
    
    def get_data_quality_metrics(self):
        """Get data quality metrics"""
        query = """
        WITH wb_quality AS (
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN gdp_value IS NOT NULL THEN 1 END) as complete_gdp,
                COUNT(CASE WHEN country_name IS NOT NULL THEN 1 END) as complete_country,
                MAX(created_at) as last_update
            FROM etl_schema.world_bank_gdp
        ),
        local_quality AS (
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN value_1 IS NOT NULL AND value_2 IS NOT NULL THEN 1 END) as complete_values,
                COUNT(CASE WHEN category IS NOT NULL THEN 1 END) as complete_category,
                MAX(created_at) as last_update
            FROM etl_schema.local_data
        ),
        duplicates AS (
            SELECT COUNT(*) as duplicate_count
            FROM (
                SELECT country_code, year, COUNT(*)
                FROM etl_schema.world_bank_gdp
                GROUP BY country_code, year
                HAVING COUNT(*) > 1
            ) dups
        )
        SELECT 
            wb.total_records as wb_total,
            (wb.complete_gdp::float / wb.total_records * 100) as wb_completeness,
            local.total_records as local_total,
            (local.complete_values::float / local.total_records * 100) as local_completeness,
            dups.duplicate_count as duplicates,
            EXTRACT(DAYS FROM CURRENT_TIMESTAMP - GREATEST(wb.last_update, local.last_update)) as data_age_days
        FROM wb_quality wb, local_quality local, duplicates dups
        """
        try:
            df = pd.read_sql(query, self.engine)
            return df.iloc[0].to_dict() if not df.empty else {}
        except Exception as e:
            logging.error(f"Error getting data quality metrics: {e}")
            return {}
    
    def get_data_quality_issues(self):
        """Get specific data quality issues"""
        query = """
        SELECT 'Missing GDP Values' as issue_type, COUNT(*) as count
        FROM etl_schema.world_bank_gdp
        WHERE gdp_value IS NULL
        
        UNION ALL
        
        SELECT 'Missing Country Names' as issue_type, COUNT(*) as count
        FROM etl_schema.world_bank_gdp
        WHERE country_name IS NULL
        
        UNION ALL
        
        SELECT 'Negative GDP Values' as issue_type, COUNT(*) as count
        FROM etl_schema.world_bank_gdp
        WHERE gdp_value < 0
        
        UNION ALL
        
        SELECT 'Missing Local Data Values' as issue_type, COUNT(*) as count
        FROM etl_schema.local_data
        WHERE value_1 IS NULL OR value_2 IS NULL
        
        UNION ALL
        
        SELECT 'Missing Categories' as issue_type, COUNT(*) as count
        FROM etl_schema.local_data
        WHERE category IS NULL
        
        ORDER BY count DESC
        """
        try:
            df = pd.read_sql(query, self.engine)
            return df[df['count'] > 0]  # Only return issues that exist
        except Exception as e:
            logging.error(f"Error getting data quality issues: {e}")
            return pd.DataFrame()
    
    def get_data_lineage(self):
        """Get data lineage information"""
        try:
            lineage_info = {
                "world_bank_data": {
                    "source": "World Bank API",
                    "extraction_frequency": "Weekly",
                    "last_processed": self._get_last_processing_time('world_bank_gdp'),
                    "processing_steps": [
                        "API Extraction",
                        "Data Validation",
                        "Ranking Calculation",
                        "Growth Rate Calculation",
                        "Database Load"
                    ]
                },
                "local_data": {
                    "source": "Local CSV Files",
                    "extraction_frequency": "Daily",
                    "last_processed": self._get_last_processing_time('local_data'),
                    "processing_steps": [
                        "CSV File Reading",
                        "Spark Transformation",
                        "Data Cleaning",
                        "Aggregation",
                        "Database Load"
                    ]
                },
                "ml_models": {
                    "source": "Combined Dataset",
                    "training_frequency": "Monthly",
                    "last_trained": self._get_last_model_training(),
                    "pipeline_steps": [
                        "Feature Engineering",
                        "Data Preprocessing",
                        "Model Training",
                        "Model Validation",
                        "Model Storage"
                    ]
                }
            }
            return lineage_info
        except Exception as e:
            logging.error(f"Error getting data lineage: {e}")
            return {}
    
    def _get_last_processing_time(self, table_name):
        """Get last processing time for a table"""
        query = f"""
        SELECT MAX(created_at) as last_processed
        FROM etl_schema.{table_name}
        """
        try:
            df = pd.read_sql(query, self.engine)
            last_processed = df.iloc[0]['last_processed']
            return last_processed.isoformat() if last_processed else None
        except Exception as e:
            logging.error(f"Error getting last processing time for {table_name}: {e}")
            return None
    
    def _get_last_model_training(self):
        """Get last model training time"""
        query = """
        SELECT MAX(training_date) as last_training
        FROM etl_schema.model_metadata
        WHERE is_active = true
        """
        try:
            df = pd.read_sql(query, self.engine)
            last_training = df.iloc[0]['last_training']
            return last_training.isoformat() if last_training else None
        except Exception as e:
            logging.error(f"Error getting last model training time: {e}")
            return None