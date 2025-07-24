from datetime import datetime, timedelta
from io import StringIO, BytesIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import logging
from minio import Minio
import subprocess
import numpy as np

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
dag = DAG(
    'etl_local_csv_pipeline',
    default_args=default_args,
    description='ETL pipeline for Chilean exports data (FOB)',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'local', 'exports', 'chile', 'fob']
)

def check_csv_files():
    """Check if CSV files are available"""
    data_dir = '/opt/airflow/data'
    
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Data directory {data_dir} does not exist")
    
    # Find all CSV files
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    if not csv_files:
        raise FileNotFoundError("No CSV files found in data directory")
    
    logging.info(f"Found CSV files: {csv_files}")
    
    # Validate that files have export data structure
    valid_files = []
    for file in csv_files:
        file_path = os.path.join(data_dir, file)
        try:
            # Try multiple encodings for Chilean CSV files
            df_sample = None
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            
            for encoding in encodings:
                try:
                    df_sample = pd.read_csv(file_path, nrows=5, encoding=encoding)
                    logging.info(f"✓ {file} - Successfully read with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df_sample is None:
                logging.error(f"⚠ {file} - Could not read with any encoding")
                continue
            
            # Check for key export columns - more flexible matching
            file_columns = [col.strip().upper() for col in df_sample.columns]
            
            # Look for key Chilean export indicators
            has_fob = any('FOB' in col and ('US$' in col or 'USD' in col or 'DOLAR' in col) for col in file_columns)
            has_country = any('PAIS' in col and 'DESTINO' in col for col in file_columns)
            has_product = any('PRODUCTO' in col for col in file_columns)
            
            if has_fob and has_country:  # At least FOB and destination country
                valid_files.append(file)
                logging.info(f"✓ {file} - Valid Chilean export data file")
                logging.info(f"  Columns found: {', '.join(file_columns[:10])}...")  # Show first 10 columns
            else:
                logging.warning(f"⚠ {file} - Doesn't match expected export data structure")
                logging.warning(f"  Has FOB: {has_fob}, Has Country: {has_country}, Has Product: {has_product}")
                
        except Exception as e:
            logging.error(f"Error reading {file}: {str(e)}")
    
    if not valid_files:
        logging.error("No valid export data files found after checking all CSVs")
        logging.error("Files checked: " + ", ".join(csv_files))
        raise ValueError("No valid export data files found")
    
    logging.info(f"Valid export data files: {valid_files}")
    return valid_files

def extract_and_upload_to_minio():
    """Extract export CSV files and upload to MinIO"""
    # MinIO connection
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
    
    data_dir = '/opt/airflow/data'
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    uploaded_files = []
    
    for file in csv_files:
        file_path = os.path.join(data_dir, file)
        try:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(file_path, encoding='latin-1')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='cp1252')
            
            logging.info(f"File {file} loaded with {len(df)} rows and {len(df.columns)} columns")
            
            df.columns = [col.strip().upper() for col in df.columns]
 
            df['SOURCE_FILE'] = file
            df['EXTRACTION_DATE'] = datetime.now()
            
            csv_string = df.to_csv(index=False)
            csv_bytes = csv_string.encode('utf-8')

            object_name = f"raw/local_data/{file}"
            minio_client.put_object(
                bucket_name,
                object_name,
                data=BytesIO(csv_bytes),  # ← ARREGLADO
                length=len(csv_bytes),
                content_type='text/csv'
        )
            
            uploaded_files.append(object_name)
            logging.info(f"Uploaded {file} to MinIO as {object_name}")
            
        except Exception as e:
            logging.error(f"Error processing {file}: {str(e)}")
            continue
    
    if not uploaded_files:
        raise ValueError("No files were successfully uploaded")
    
    return uploaded_files

def trigger_spark_transformation():
    """Trigger Spark job for Chilean export data transformation"""
    import subprocess
    import docker
    
    try:
        # Use docker client to execute spark-submit in the spark-master container
        docker_cmd = [
            "docker", "exec", "spark_master",
            "/opt/bitnami/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1034",
            "/opt/bitnami/spark/spark_jobs/transform_local_data.py"
        ]
        
        logging.info(f"Executing Spark job with command: {' '.join(docker_cmd)}")
        
        result = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=1800)
        
        if result.returncode != 0:
            logging.error(f"Spark job failed: {result.stderr}")
            logging.error(f"Spark stdout: {result.stdout}")
            raise Exception(f"Spark job failed: {result.stderr}")
        
        logging.info("Spark transformation completed successfully")
        logging.info(f"Spark output: {result.stdout}")
        
        return True
        
    except subprocess.TimeoutExpired:
        logging.error("Spark job timed out after 30 minutes")
        raise Exception("Spark job timed out")
    except Exception as e:
        logging.error(f"Error running Spark job: {str(e)}")
        # Fallback to pandas processing
        logging.info("Falling back to pandas processing...")
        return trigger_pandas_transformation()

def trigger_pandas_transformation():
    """Fallback: Process data using pandas instead of Spark"""
    
    logging.info("Starting pandas transformation as Spark fallback...")
    
    # MinIO connection
    minio_client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    
    bucket_name = 'etl-data'
    
    try:
        # List all CSV files in raw/local_data/
        objects = minio_client.list_objects(bucket_name, prefix='raw/local_data/', recursive=True)
        
        all_dataframes = []
        
        for obj in objects:
            if obj.object_name.endswith('.csv'):
                logging.info(f"Processing {obj.object_name}")
                
                # Read CSV from MinIO
                response = minio_client.get_object(bucket_name, obj.object_name)
                df = pd.read_csv(response)
                
                # Basic transformations
                df.columns = [col.strip().upper() for col in df.columns]
                
                # Clean FOB values
                if 'US$ FOB' in df.columns:
                    df['US$ FOB'] = pd.to_numeric(df['US$ FOB'], errors='coerce')
                    df = df[df['US$ FOB'] > 0]  # Filter valid FOB values
                
                # Add processing metadata
                df['PROCESSED_AT'] = datetime.now()
                df['PROCESSING_METHOD'] = 'PANDAS_FALLBACK'
                
                all_dataframes.append(df)
                logging.info(f"Processed {len(df)} records from {obj.object_name}")
        
        if all_dataframes:
            # Combine all DataFrames
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            logging.info(f"Combined {len(combined_df)} total records")
            
            # Upload processed data back to MinIO
            csv_string = combined_df.to_csv(index=False)
            processed_object_name = f"processed/local_data/processed_export_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            minio_client.put_object(
                bucket_name,
                processed_object_name,
                data=BytesIO(csv_string.encode('utf-8')),
                length=len(csv_string.encode('utf-8')),
                content_type='text/csv'
            )
            
            logging.info(f"SUCCESS: Processed data uploaded to {processed_object_name}")
            logging.info(f"Total records processed: {len(combined_df)}")
            
            return True
        else:
            logging.error("No data files found to process")
            return False
            
    except Exception as e:
        logging.error(f"Error in pandas transformation: {str(e)}")
        raise

def load_to_postgres():
    """Load processed export data to PostgreSQL"""
    # MinIO connection
    minio_client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )
    

    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        bucket_name = 'etl-data'
        objects = minio_client.list_objects(bucket_name, prefix='processed/local_data/', recursive=True)
        
        total_loaded = 0
        
        for obj in objects:
            if obj.object_name.endswith('.csv'):
                logging.info(f"Loading {obj.object_name} to PostgreSQL")
                
                response = minio_client.get_object(bucket_name, obj.object_name)
                df = pd.read_csv(response)
                
                if not df.empty:
                    engine = postgres_hook.get_sqlalchemy_engine()
                    
                    df.to_sql(
                        'local_data',
                        engine,
                        schema='etl_schema',
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    
                    total_loaded += len(df)
                    logging.info(f"Loaded {len(df)} rows from {obj.object_name}")
        
        logging.info(f"Total loaded: {total_loaded} export records to PostgreSQL")
        
        stats_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT source_file) as total_files,
            MIN(extraction_date) as first_extraction,
            MAX(extraction_date) as last_extraction
        FROM etl_schema.local_data
        WHERE extraction_date >= CURRENT_DATE
        """
        
        result = postgres_hook.get_first(stats_query)
        logging.info(f"Load statistics: {result}")
        
    except Exception as e:
        logging.error(f"Error loading to PostgreSQL: {str(e)}")
        raise

def validate_export_data():
    """Validate loaded export data quality"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    quality_checks = [
        {
            'name': 'Check for records loaded today',
            'query': """
                SELECT COUNT(*) as count
                FROM etl_schema.local_data
                WHERE extraction_date >= CURRENT_DATE
            """,
            'min_expected': 1
        },
        {
            'name': 'Check for valid FOB values',
            'query': """
                SELECT COUNT(*) as count
                FROM etl_schema.local_data
                WHERE extraction_date >= CURRENT_DATE
                AND (
                    "US$ FOB" IS NULL OR 
                    "US$ FOB" < 0 OR
                    "US$ FOB" = ''
                )
            """,
            'max_expected': 0.1  
        },
        {
            'name': 'Check for destination countries',
            'query': """
                SELECT COUNT(DISTINCT "PAIS DE DESTINO") as unique_countries
                FROM etl_schema.local_data
                WHERE extraction_date >= CURRENT_DATE
                AND "PAIS DE DESTINO" IS NOT NULL
                AND "PAIS DE DESTINO" != ''
            """,
            'min_expected': 5 
        }
    ]
    
    validation_results = []
    
    for check in quality_checks:
        try:
            result = postgres_hook.get_first(check['query'])
            result_value = list(result.values())[0] if result else 0
            
            if 'min_expected' in check:
                passed = result_value >= check['min_expected']
                status = "✅ PASS" if passed else "❌ FAIL"
                validation_results.append(f"{check['name']}: {status} (got {result_value}, expected >= {check['min_expected']})")
            elif 'max_expected' in check:
                total_records = postgres_hook.get_first("""
                    SELECT COUNT(*) FROM etl_schema.local_data 
                    WHERE extraction_date >= CURRENT_DATE
                """)[0]
                percentage = (result_value / total_records * 100) if total_records > 0 else 0
                passed = percentage <= check['max_expected'] * 100
                status = "✅ PASS" if passed else "❌ FAIL"
                validation_results.append(f"{check['name']}: {status} (got {percentage:.1f}%, expected <= {check['max_expected']*100}%)")
            
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
        logging.info("All export data quality checks passed!")
    
    return validation_results

check_files_task = PythonOperator(
    task_id='check_csv_files',
    python_callable=check_csv_files,
    dag=dag
)

extract_upload_task = PythonOperator(
    task_id='extract_and_upload_to_minio',
    python_callable=extract_and_upload_to_minio,
    dag=dag
)

transform_task = PythonOperator(
    task_id='trigger_spark_transformation',
    python_callable=trigger_spark_transformation,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_export_data',
    python_callable=validate_export_data,
    dag=dag
)

check_files_task >> extract_upload_task >> transform_task >> load_task >> validate_task