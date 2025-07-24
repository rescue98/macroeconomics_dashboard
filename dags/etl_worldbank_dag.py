from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests
import logging
from minio import Minio
from io import StringIO
import numpy as np
import time

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

# DAG definition
dag = DAG(
    'etl_worldbank_gdp_pipeline',
    default_args=default_args,
    description='ETL pipeline for World Bank GDP data (2024 focus)',
    schedule_interval=timedelta(days=7),
    catchup=False,
    tags=['etl', 'worldbank', 'gdp', 'api', '2024']
)

def extract_worldbank_gdp_data():
    """Extract GDP data from World Bank API"""
    base_url = "https://api.worldbank.org/v2"
    indicator = "NY.GDP.MKTP.CD" 
    
    all_data = []

    years_to_extract = [2024, 2023, 2022, 2021]
    
    for year in years_to_extract:
        try:
            logging.info(f"Extracting World Bank GDP data for {year}...")
            
            url = f"{base_url}/country/all/indicator/{indicator}"
            params = {
                'format': 'json',
                'date': str(year),
                'per_page': 500,
                'page': 1
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if len(data) >= 2 and isinstance(data[1], list):
                records = data[1]
                
                valid_records = []
                for record in records:
                    if (record.get('value') is not None and 
                        record.get('value') != '' and
                        record.get('country', {}).get('value') and
                        record.get('countryiso3code')):
                        
                        valid_records.append({
                            'country_code': record.get('countryiso3code'),
                            'country_name': record.get('country', {}).get('value'),
                            'indicator_code': record.get('indicator', {}).get('id'),
                            'indicator_name': record.get('indicator', {}).get('value'),
                            'year': int(record.get('date')),
                            'gdp_value': float(record.get('value')),
                            'extraction_date': datetime.now().isoformat()
                        })
                
                logging.info(f"Year {year}: Found {len(valid_records)} valid GDP records")
                all_data.extend(valid_records)
                
             
                if len(valid_records) > 100:
                    break
                    
            else:
                logging.warning(f"No valid data structure for year {year}")
            time.sleep(2)
                
        except requests.RequestException as e:
            logging.error(f"Error fetching data for year {year}: {str(e)}")
            continue
        except Exception as e:
            logging.error(f"Unexpected error for year {year}: {str(e)}")
            continue
    
    if not all_data:
        raise ValueError("No valid World Bank GDP data could be extracted")
    
    logging.info(f"Total extracted: {len(all_data)} GDP records")
    return all_data

def upload_to_minio(**context):
    """Upload extracted data to MinIO"""
    data = context['task_instance'].xcom_pull(task_ids='extract_worldbank_data')
    
    if not data:
        raise ValueError("No data to upload to MinIO")
    
    df = pd.DataFrame(data)
    
    minio_client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    bucket_name = 'etl-data'
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        logging.info(f"Created bucket: {bucket_name}")

    csv_string = df.to_csv(index=False)
    csv_bytes = csv_string.encode('utf-8')
    
    object_name = f"raw/worldbank_data/gdp_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    minio_client.put_object(
        bucket_name,
        object_name,
        data=StringIO(csv_string),
        length=len(csv_bytes),
        content_type='text/csv'
    )
    
    logging.info(f"Uploaded World Bank GDP data to MinIO: {object_name}")
    return object_name

def transform_worldbank_data(**context):
    """Transform World Bank data using pandas"""
    object_name = context['task_instance'].xcom_pull(task_ids='upload_to_minio')
    
    minio_client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    bucket_name = 'etl-data'
    response = minio_client.get_object(bucket_name, object_name)
    df = pd.read_csv(response)
    
    logging.info(f"Starting transformation with {len(df)} records")
    
    # 1. Data cleaning and validation
    df = df.dropna(subset=['gdp_value', 'country_code'])
    df = df[df['gdp_value'] > 0]
    
    # Remove aggregates and non-countries (World Bank includes regional aggregates)
    exclude_codes = [
        'WLD', 'HIC', 'UMC', 'LMC', 'LIC', 'MIC', 'LCN', 'EAS', 'ECS', 'MEA', 
        'NAC', 'SAS', 'SSF', 'ARB', 'CSS', 'CEB', 'EAR', 'EAP', 'TEA', 'EMU',
        'EUU', 'FCS', 'HPC', 'IBD', 'IBT', 'IDB', 'IDX', 'IDA', 'LDC', 'LTE',
        'OED', 'OSS', 'PRE', 'PSS', 'PST', 'TSA', 'TSS'
    ]
    df = df[~df['country_code'].isin(exclude_codes)]
    
    logging.info(f"After cleaning: {len(df)} records")
    
    # 2. Calculate rankings for each year
    df['ranking'] = df.groupby('year')['gdp_value'].rank(method='dense', ascending=False).astype(int)
    
    # 3. Calculate GDP in billions for readability
    df['gdp_billion_usd'] = df['gdp_value'] / 1e9
    
    # 4. Calculate logarithmic GDP for analysis
    df['log_gdp'] = np.log(df['gdp_value'])
    
    # 5. Calculate growth rates (year-over-year)
    df = df.sort_values(['country_code', 'year'])
    df['gdp_growth_rate'] = df.groupby('country_code')['gdp_value'].pct_change() * 100
    
    # 6. Add regional classification (expanded)
    def assign_region(country_code):
        north_america = ['USA', 'CAN', 'MEX']
        south_america = ['BRA', 'ARG', 'CHL', 'COL', 'PER', 'VEN', 'ECU', 'URY', 'PRY', 'BOL', 'GUY', 'SUR']
        europe = ['DEU', 'FRA', 'GBR', 'ITA', 'ESP', 'NLD', 'CHE', 'BEL', 'AUT', 'SWE', 'NOR', 'DNK', 'FIN', 'IRL', 'PRT', 'GRC', 'POL', 'CZE', 'HUN', 'SVK', 'SVN', 'EST', 'LVA', 'LTU', 'LUX', 'MLT', 'CYP', 'BGR', 'ROU', 'HRV', 'RUS']
        asia = ['CHN', 'JPN', 'IND', 'KOR', 'IDN', 'TUR', 'TWN', 'THA', 'ISR', 'MYS', 'SGP', 'PHL', 'VNM', 'BGD', 'PAK', 'LKA', 'MMR', 'KHM', 'LAO', 'BRN', 'NPL', 'AFG', 'BTN', 'MDV']
        africa = ['NGA', 'ZAF', 'EGY', 'DZA', 'AGO', 'KEN', 'ETH', 'GHA', 'TUN', 'TZA', 'UGA', 'CMR', 'CIV', 'ZMB', 'SEN', 'MDG', 'BFA', 'MLI', 'MWI', 'MOZ', 'NER', 'RWA', 'GIN', 'BEN', 'BDI', 'TCD', 'SLE', 'TGO', 'LBR', 'MRT', 'LSO', 'GNB', 'CAF', 'GMB', 'SWZ', 'GNQ', 'DJI', 'COM', 'CPV', 'STP']
        oceania = ['AUS', 'NZL', 'PNG', 'FJI', 'NCL', 'SLB', 'VUT', 'WSM', 'KIR', 'FSM', 'TON', 'PLW', 'MHL', 'TUV', 'NRU']
        middle_east = ['SAU', 'IRN', 'ARE', 'IRQ', 'QAT', 'KWT', 'OMN', 'JOR', 'LBN', 'BHR', 'PSE', 'YEM', 'SYR']
        
        if country_code in north_america:
            return 'North America'
        elif country_code in south_america:
            return 'South America'  
        elif country_code in europe:
            return 'Europe'
        elif country_code in asia:
            return 'Asia'
        elif country_code in africa:
            return 'Africa'
        elif country_code in oceania:
            return 'Oceania'
        elif country_code in middle_east:
            return 'Middle East'
        else:
            return 'Other'
    
    df['region'] = df['country_code'].apply(assign_region)
    
    # 7. Add economy size classification
    def classify_economy_size(gdp_value):
        if gdp_value >= 5e12:
            return 'Major'
        elif gdp_value >= 1e12:  
            return 'Large'
        elif gdp_value >= 5e11:
            return 'Upper Middle'
        elif gdp_value >= 1e11: 
            return 'Middle'
        elif gdp_value >= 5e10:  
            return 'Lower Middle'
        else: 
            return 'Small'
    
    df['economy_size'] = df['gdp_value'].apply(classify_economy_size)
    
    # 8. Add processing metadata
    df['created_at'] = datetime.now()
    df['updated_at'] = datetime.now()
    df['processed_at'] = datetime.now()
    
    # 9. Sort by most recent year and ranking
    df = df.sort_values(['year', 'ranking'])
    
    logging.info(f"Transformation completed: {len(df)} records")
    logging.info(f"Years covered: {sorted(df['year'].unique())}")
    logging.info(f"Countries: {df['country_code'].nunique()}")
    logging.info(f"Regions: {sorted(df['region'].unique())}")
    
    # Upload processed data back to MinIO
    processed_csv = df.to_csv(index=False)
    processed_object_name = f"processed/worldbank_data/processed_gdp_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    minio_client.put_object(
        bucket_name,
        processed_object_name,
        data=StringIO(processed_csv),
        length=len(processed_csv.encode('utf-8')),
        content_type='text/csv'
    )
    
    logging.info(f"Processed World Bank data uploaded to MinIO: {processed_object_name}")
    return processed_object_name

def load_to_postgres(**context):
    """Load processed World Bank data to PostgreSQL"""
    processed_object_name = context['task_instance'].xcom_pull(task_ids='transform_worldbank_data')
    

    minio_client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    bucket_name = 'etl-data'
    response = minio_client.get_object(bucket_name, processed_object_name)
    df = pd.read_csv(response)
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    try:
        years_to_update = df['year'].unique().tolist()
        years_str = ','.join(map(str, years_to_update))
        
        delete_query = f"""
        DELETE FROM etl_schema.world_bank_gdp 
        WHERE year IN ({years_str})
        """
        
        postgres_hook.run(delete_query)
        logging.info(f"Deleted existing data for years: {years_to_update}")
        
        df.to_sql(
            'world_bank_gdp',
            engine,
            schema='etl_schema',
            if_exists='append',
            index=False,
            method='multi',
            chunksize=100
        )
        
        logging.info(f"Loaded {len(df)} World Bank GDP records to PostgreSQL")
        
        stats_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT country_code) as total_countries,
            COUNT(DISTINCT year) as total_years,
            MIN(year) as min_year,
            MAX(year) as max_year,
            COUNT(DISTINCT region) as total_regions,
            AVG(gdp_value) as avg_gdp,
            MAX(gdp_value) as max_gdp
        FROM etl_schema.world_bank_gdp
        """
        
        result = postgres_hook.get_first(stats_query)
        logging.info(f"PostgreSQL load statistics: {dict(result)}")
        
    except Exception as e:
        logging.error(f"Error loading to PostgreSQL: {str(e)}")
        raise

def validate_worldbank_data():
    """Validate loaded World Bank data quality"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Data quality checks
    quality_checks = [
        {
            'name': 'Check for recent data',
            'query': """
                SELECT COUNT(*) as count
                FROM etl_schema.world_bank_gdp
                WHERE year >= 2022
            """,
            'min_expected': 100
        },
        {
            'name': 'Check GDP value validity',
            'query': """
                SELECT COUNT(*) as count
                FROM etl_schema.world_bank_gdp
                WHERE gdp_value <= 0 OR gdp_value IS NULL
            """,
            'max_expected': 0
        },
        {
            'name': 'Check ranking consistency',
            'query': """
                SELECT COUNT(*) as count
                FROM etl_schema.world_bank_gdp
                WHERE ranking <= 0 OR ranking IS NULL
            """,
            'max_expected': 0
        },
        {
            'name': 'Check for top economies',
            'query': """
                SELECT COUNT(*) as count
                FROM etl_schema.world_bank_gdp
                WHERE ranking <= 10 AND year = (SELECT MAX(year) FROM etl_schema.world_bank_gdp)
            """,
            'min_expected': 10
        },
        {
            'name': 'Check for duplicate country-year combinations',
            'query': """
                SELECT COUNT(*) as count
                FROM (
                    SELECT country_code, year
                    FROM etl_schema.world_bank_gdp
                    GROUP BY country_code, year
                    HAVING COUNT(*) > 1
                ) duplicates
            """,
            'max_expected': 0
        }
    ]
    
    validation_results = []
    
    for check in quality_checks:
        try:
            result = postgres_hook.get_first(check['query'])
            result_value = result['count'] if result else 0
            
            if 'min_expected' in check:
                passed = result_value >= check['min_expected']
                status = "✅ PASS" if passed else "❌ FAIL"
                validation_results.append(f"{check['name']}: {status} (got {result_value}, expected >= {check['min_expected']})")
            elif 'max_expected' in check:
                passed = result_value <= check['max_expected']
                status = "✅ PASS" if passed else "❌ FAIL"
                validation_results.append(f"{check['name']}: {status} (got {result_value}, expected <= {check['max_expected']})")
            
            logging.info(f"{check['name']}: {result_value}")
            
        except Exception as e:
            logging.error(f"Error running quality check '{check['name']}': {str(e)}")
            validation_results.append(f"{check['name']}: ❌ ERROR - {str(e)}")
    
    for result in validation_results:
        logging.info(result)
    
    failed_checks = [r for r in validation_results if "❌" in r]
    if failed_checks:
        logging.warning(f"Some quality checks failed: {len(failed_checks)}/{len(quality_checks)}")
        for failure in failed_checks:
            logging.warning(failure)
    else:
        logging.info("All World Bank GDP data quality checks passed!")
    
    return validation_results

extract_task = PythonOperator(
    task_id='extract_worldbank_data',
    python_callable=extract_worldbank_gdp_data,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_worldbank_data',
    python_callable=transform_worldbank_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_worldbank_data,
    dag=dag
)

extract_task >> upload_task >> transform_task >> load_task >> validate_task